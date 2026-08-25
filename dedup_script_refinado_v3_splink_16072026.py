"""
Probabilistic Record Linkage System for Brazilian Health Data SUS
====================================================

This script performs probabilistic deduplication of individual records in health databases,
using record linkage with DuckDB and Splink. The complete process includes:

1. Reading and automatic encoding detection for multiple formats (CSV, DBF, Parquet, Excel, DBC e TXT)
2. Data standardization and cleaning (names, dates, sex, municipality)
3. Probabilistic deduplication with Splink using Expectation-Maximization algorithm
4. Rule-based decision refinement system for borderline pairs
5. Transitive clustering of duplicate records
6. Generation of final datasets with preserved original data

Authors: Dayan Oliveira, Alexandre Vilhena, Mário Junior e Julia Stefanie
Last update: 29 Maio de 2026
"""
from __future__ import annotations

import duckdb
import re
import csv
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple
from enum import Enum
from datetime import datetime
import chardet
import unicodedata
from splink import DuckDBAPI, Linker, SettingsCreator, block_on
from splink.blocking_rule_library import CustomRule
import psutil
import os
import pandas as pd
import polars as pl
import json
import time
import math
import gc
import sys

# ========================================================================================
# LOG SYSTEM CONFIGURATION
# ========================================================================================

# This system captures all console output (stdout and stderr) and saves to a .txt file
# with timestamp. Enables complete traceability of processing and debugging.

class Logger:
    """
    Class to duplicate console output to log file.   
    Captures all print() messages and errors, saving simultaneously
    to terminal and file. Essential for auditing long-running processes.
    """
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, 'w', encoding='utf-8')
    
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
    
    def flush(self):
        self.terminal.flush()
        self.log.flush()

# Create log file with unique timestamp for each execution
log_filename = f"log_deduplicacao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
sys.stdout = Logger(log_filename)
sys.stderr = sys.stdout

print(f"Log will be saved at: {log_filename}")
print("="*60 + "\n")

# ========================================================================================
# MEMORY MANAGEMENT AND DUCKDB CONFIGURATION
# ========================================================================================

# DuckDB can process data larger than RAM through disk spillover.
# These functions configure adaptive memory limits based on available system RAM,
# ensuring stable processing even with large data volumes.

def get_available_memory_gb() -> float:
    """Get available system memory in GB
    Uses psutil to query virtual memory available at call time.
    Important: returns AVAILABLE memory, not total system memory.
    """
    mem = psutil.virtual_memory()
    available_gb = mem.available / (1024 ** 3)
    return available_gb

# Variáveis globais para controle de tempo
_block_start_time = None
_last_step_time = None
_block_peak_ram = 0.0
_block_name = ""
_block_registros = 0
_block_pares_gerados = 0
_block_pares_aprovados = 0

def monitor(etapa: str, bloco: str = "", registros: int = 0, pares: int = 0, is_final: bool = False):
    """
    Monitora memória e tempo em formato padronizado.
    Formato: [MONITOR] etapa | Bloco: X | Registros: X | Pares: X | RAM: X.XXGB (XX.X%) | Tempo etapa: Xs | Tempo acum: Xs
    
    Args:
        etapa: Nome da etapa (ex: "01_dados_carregados")
        bloco: Identificador do bloco (ex: "1990" ou "1990-1992")
        registros: Número de registros no bloco
        pares: Número de pares (gerados ou aprovados, dependendo da etapa)
        is_final: Se True, exibe resumo do bloco
    """
    global _block_start_time, _last_step_time, _block_peak_ram
    global _block_name, _block_registros, _block_pares_gerados, _block_pares_aprovados
    
    current_time = time.time()
    mem = psutil.virtual_memory()
    usado_gb = mem.used / (1024 ** 3)
    total_gb = mem.total / (1024 ** 3)
    
    # Atualiza pico de RAM do bloco
    if usado_gb > _block_peak_ram:
        _block_peak_ram = usado_gb
    
    # Atualiza informações do bloco
    if bloco:
        _block_name = bloco
    if registros > 0:
        _block_registros = registros
    
    # Calcula tempos
    if _last_step_time is None:
        tempo_etapa = 0.0
        tempo_acum = 0.0
    else:
        tempo_etapa = current_time - _last_step_time
        tempo_acum = current_time - _block_start_time if _block_start_time else 0.0
    
    _last_step_time = current_time
    
    # Monta a linha de log
    parts = [f"[MONITOR] {etapa}"]
    if _block_name:
        parts.append(f"Bloco: {_block_name}")
    if _block_registros > 0:
        parts.append(f"Registros: {_block_registros:,}")
    if pares > 0:
        parts.append(f"Pares: {pares:,}")
    parts.append(f"RAM: {usado_gb:.1f}GB ({mem.percent:.1f}%)")
    parts.append(f"Tempo etapa: {tempo_etapa:.1f}s")
    parts.append(f"Tempo acum: {tempo_acum:.1f}s")
    
    print("    " + " | ".join(parts))
    
    # Se é etapa final, exibe resumo do bloco
    if is_final:
        _print_block_summary(tempo_acum)

def monitor_init_block(bloco: str):
    """
    Inicializa o monitoramento para um novo bloco.
    DEVE ser chamado no início de process_year_block.
    """
    global _block_start_time, _last_step_time, _block_peak_ram
    global _block_name, _block_registros, _block_pares_gerados, _block_pares_aprovados
    
    _block_start_time = time.time()
    _last_step_time = _block_start_time
    _block_peak_ram = 0.0
    _block_name = bloco
    _block_registros = 0
    _block_pares_gerados = 0
    _block_pares_aprovados = 0

def monitor_set_pares_gerados(pares: int):
    """Registra o número de pares gerados pelo Splink."""
    global _block_pares_gerados
    _block_pares_gerados = pares

def monitor_set_pares_aprovados(pares: int):
    """Registra o número de pares aprovados."""
    global _block_pares_aprovados
    _block_pares_aprovados = pares

def _print_block_summary(tempo_total: float):
    """Imprime resumo consolidado do bloco."""
    global _block_name, _block_registros, _block_pares_gerados, _block_pares_aprovados, _block_peak_ram
    
    # Formata tempo
    if tempo_total >= 60:
        minutos = int(tempo_total // 60)
        segundos = int(tempo_total % 60)
        tempo_fmt = f"{minutos}m {segundos}s"
    else:
        tempo_fmt = f"{tempo_total:.1f}s"
    
    print(f"    [MONITOR] {'='*60}")
    print(f"    [MONITOR] RESUMO BLOCO {_block_name}")
    print(f"    [MONITOR] Registros: {_block_registros:,} | Pares gerados: {_block_pares_gerados:,} | Pares aprovados: {_block_pares_aprovados:,}")
    print(f"    [MONITOR] Pico RAM: {_block_peak_ram:.1f}GB | Tempo total: {tempo_fmt}")
    print(f"    [MONITOR] {'='*60}")

def configure_duckdb_memory(con: duckdb.DuckDBPyConnection, memory_percentage: float = 0.75) -> str:
    """Configure DuckDB to use a percentage of available memory with disk spillover
    DuckDB is configured to:
    1. Use up to memory_percentage% of available RAM (default 75%)
    2. Spillover to disk when exceeding the limit
    3. Disable object cache to free memory
    """
    available_gb = get_available_memory_gb()
    memory_limit_gb = available_gb * memory_percentage
    memory_limit_gb = max(1.0, min(memory_limit_gb, available_gb))
    
    # Create temporary directory for spillover (operations exceeding RAM)
    temp_dir = "./temp_duckdb"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Configure DuckDB limits and behavior:

    # 1. memory_limit: defines max RAM before spilling to disk
    con.execute(f"SET memory_limit = '{memory_limit_gb:.1f}GB'")
    # 2. temp_directory: where to write temporary data during spillover
    con.execute(f"SET temp_directory = '{temp_dir}'")
    # 3. enable_object_cache: disable object caching to save RAM
    con.execute("SET enable_object_cache = false")
    
    config_msg = (
        f"DuckDB Memory Configuration:\n"
        f"  Available memory: {available_gb:.2f} GB\n"
        f"  Memory limit set: {memory_limit_gb:.2f} GB ({memory_percentage*100:.0f}%)\n"
        f"  Temp directory: {temp_dir}\n"
        f"  Spillover to disk: ENABLED"
    )
    
    return config_msg

# ========================================================================================
# USER CONFIGURATION AND DATA STRUCTURES
# ========================================================================================

# This section defines all configurable parameters for the deduplication process,
# including variable name mappings, comparison thresholds, blocking strategies,
# and data quality rules. Users should modify UserConfig parameters to adapt
# the system to their specific datasets and linkage needs.

def normalize_text(text: str) -> str:
    """Remove accents and special characters, convert to lowercase
    
    The normalization is used for name cleaning and standardization.
    Process:
    1. Decompose unicode characters (NFD normalization) - i.e. Canonical Decomposition to separate accents
    2. Remove diacritical marks (accents, tildes, etc.)
    3. Remove all non-alphabetic characters except spaces
    4. Convert to lowercase
    5. Collapse multiple spaces into single spaces
    """
    if not text:
        return ""
    nfd = unicodedata.normalize('NFD', text)
    without_accents = ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')
    cleaned = re.sub(r'[^a-zA-Z ]', ' ', without_accents)
    result = ' '.join(cleaned.lower().split())
    # Remover prefixos de tratamento (senhor, senhora, sr, sra)
    result = re.sub(r'^(senhora|senhor|sra|sr)\s+(?=\S+\s+\S+)', '', result).strip()
    return result

@dataclass
class UserConfig:
    """ 
    This dataclass centralizes all user-configurable settings
    """

    # ========================================================================
    # PATH CONFIGURATION
    # ========================================================================
    # Modify here the paths to your files



    # PATH TO FOLDER WITH SOURCE FILES
    data_folder: str = "./linkage_models/BASES"

    # OUTPUT FOLDERS
    complete_files_folder: str = "./linkage_models/BASES/COMPLETE_WITH_ID"
    results_folder: str = "./linkage_models/BASES/DEDUP_RESULTS"

    # DATABASE FILE
    database_file: str = "./linkage_models/BASES/dedup_database.duckdb"

    #DONT MODIFY IN HERE, KEEP SCROLLING

    # LISTS OF POSSIBLE NAMES FOR EACH VARIABLE
    possible_names_nome: List[str] = None
    possible_names_nome_mae: List[str] = None
    possible_names_nome_pai: List[str] = None
    possible_names_cod_municipio: List[str] = None
    possible_names_data_nascimento: List[str] = None
    possible_names_sexo: List[str] = None
    possible_names_idade: List[str] = None
    possible_names_data_notificacao: List[str] = None
    
    # ========================================================================
    # LEVENSHTEIN SIMILARITY THRESHOLDS (0.0 to 1.0)
    # ========================================================================
    # Control fuzzy matching strictness for each variable:
    # - 1.0 = exact match required
    # - 0.8-0.9 = high similarity (few character differences)
    # - 0.6-0.7 = moderate similarity (several character differences)
    # - 0.0 = disable comparison (ignore this variable)
    #
    # Higher thresholds reduce false positives but may miss valid matches.
    # Lower thresholds increase recall but may create false duplicates.


# LEVENSHTEIN THRESHOLDS (0.0 to 1.0)
    threshold_nome: float = 0.50
    # Particoes de salting do Splink (paraleliza o predict apesar da chave ano/sexo de baixa cardinalidade)
    salting_partitions: int = 16
    threshold_nome_mae: float = 0.0
    threshold_nome_pai: float = 0.0
    threshold_data_nascimento: float = 0.85
    threshold_cod_municipio: float = 0.0

    # ====== FAIXAS DE SCORE DE NOME PARA REFINAMENTO ======
    # Três faixas estratificadas: A (60-85), B (85-95), C (>=95).
    # Cada faixa aplica critérios próprios de primeiro nome e sobrenome.
    # Aplicado POR CAMPO (pessoa, mãe, pai): o critério depende do score do próprio campo.
    threshold_nome_faixa_b_min: float = 0.85   # piso faixa B (e teto faixa A)
    threshold_nome_faixa_c_min: float = 0.95   # piso faixa C (e teto faixa B)
    # Piso de aceitação no IDADE MODE quando ambos pais nulos (registros sem data).
    # Pares com ambos pais nulos só são aceitos se score >= 85 (faixas B ou C).
    threshold_nome_idade_mode_pais_ausentes: float = 0.85
    # Minimum full-name score to allow first-name abbreviation bypass (single-letter initial)
    # Abreviação só vale nas faixas B (>=85) e C (>=95), nunca na faixa A
    threshold_nome_abreviacao: float = 0.85

    # ====== LEVENSHTEIN DINÂMICO DE PRIMEIRO NOME ======
    # FAIXA A (60-85): mais rígido (conservador)
    threshold_primnome_faixa_a_tam3: float = 75.0
    threshold_primnome_faixa_a_tam5: float = 70.0
    threshold_primnome_faixa_a_tam8: float = 67.0
    threshold_primnome_faixa_a_tammax: float = 65.0

    # FAIXAS B (85-95) e C (>=95): tabela com tam>8 = 60
    threshold_primnome_faixa_bc_tam3: float = 65.0
    threshold_primnome_faixa_bc_tam5: float = 60.0
    threshold_primnome_faixa_bc_tam8: float = 57.0
    threshold_primnome_faixa_bc_tammax: float = 60.0

# SURNAME OVERLAP CONFIGURATION
    # Used in refinement for pairs with score between 50-95
    # Faltantes = surnames present on one side but absent on the other
    # Trocados = surnames present on both sides but different (didn't match)
    # Prepositions (DE, DA, DO, DOS, DAS) are excluded from token count
    max_sobrenome_faltante_pessoa: int = 1 #permite ter 1 sobrenome a mais
    max_sobrenome_trocado_pessoa: int = 0 #nao permite ter sobrenome contrastante
    max_sobrenome_faltante_pais: int = 2 #permite ter 2 sobrenomes a mais
    max_sobrenome_trocado_pais: int = 1 #permite ter 1 par de sobrenomes contrastantes 
    # Repescagem F3: se o sobrenome de uma entidade for reprovado, junta o nome completo
    # dela SEM espaços e compara via Levenshtein; se >= este valor (0-100), neutraliza a
    # reprovação de sobrenome daquela entidade (resgata sobrenome grudado por erro de digitação)
    threshold_sobrenome_grudado_total: int = 85

    # ========================================================================
    # BLOCKING STRATEGY CONFIGURATION
    # ========================================================================
    # Blocking reduces computational complexity by only comparing records
    # within the same exact block. Essential for large datasets.
    #
    # Trade-off: Blocking improves performance but may miss matches across
    # block boundaries (e.g., birth year data entry errors).    
    # Important: Blocking on municipality will require exact match on cod_municipio
    # This is pre-defining that individuals do not migrate within its territory

    # BLOCKING VARIABLES
    usar_blocagem_municipio: bool = False
    usar_blocagem_ano: bool = True
    usar_blocagem_sexo: bool = True

    # ========================================================================
    # SPLINK PROBABILITY THRESHOLDS
    # ========================================================================
    # Splink calculates match probabilities using Expectation-Maximization.
    # Two thresholds control decision boundaries:
    #
    # threshold_match_probability_predict: Initial threshold for predictions
    #   - Lower values = more potential matches sent to refinement
    #   - Typical range: 0.2-0.4
    #
    # threshold_match_probability_cluster: Final threshold for clustering
    #   - Higher values = stricter clustering, fewer false positives
    #   - Typical range: 0.7-0.9

    # SPLINK THRESHOLD CONFIGURATION
    # Thresholds for records WITH birth date (Passada 1)
    # Minimum probability to consider as potential match (sent to refinement)
    threshold_match_probability_predict: float = 0.6
    # Minimum probability for definitive clustering (higher = stricter)
    threshold_match_probability_cluster: float = 0.02
    
    # Thresholds for records with AGE ONLY (Passada 2 - lower thresholds needed)
    # Minimum probability to consider as potential match (sent to refinement)
    threshold_match_probability_predict_idade: float = 0.01
    # Minimum probability for definitive clustering (higher = stricter)
    threshold_match_probability_cluster_idade: float = 0.07
    
    # ========================================================================
    # TRUE MATCHES METHOD DECISION
    # ========================================================================
    
    # Splink system activation
    usar_splink: bool = True
    
    # Refinement system activation
    usar_decisao_refinada: bool = True

    # ========================================================================
    # SINASC CONFIGURATION
    # ========================================================================
    # When a SINASC database is detected (filename contains "SINASC"),
    # these flags control which individual is being deduplicated.
    # If SINASC is not detected, these flags are ignored.
    # mae=True: deduplicate mothers (NOMEMAE as individual)
    # filho=True: deduplicate children (NOMEMAE as individual, DTNASC as birth date)
    # Both True or both False with SINASC detected = ERROR

    sinasc_mae: bool = True
    sinasc_filho: bool = False

    # ========================================================================
    # BASES DE REFERÊNCIA NA SAÍDA FINAL (SIM / CADUNICO)
    # ========================================================================
    # Controlam se os registros NÃO-PAREADOS do SIM e do CADUNICO entram nas
    # bases finais (pares_todos / final_todos).
    #   True  = comportamento antigo: traz TODOS os ids dessas bases, inclusive
    #           os que não encontraram par (cada um recebe id_global único).
    #   False = essas bases servem apenas como referência para identificar
    #           pares; seus registros não-pareados NÃO entram nas bases finais.
    #           Os registros dessas bases que PAREARAM permanecem normalmente.
    # Não afeta as demais bases (SINAN etc.), que sempre trazem todos os ids.
    # Não afeta pares_c_match / final_matches (que são apenas os pares).
    SIM_BASE_FINAL: bool = False
    CADUNIC_BASE_FINAL: bool = False

    # ========================================================================
    # NAME CLEANING CONFIGURATION
    # ========================================================================
    aplicar_limpeza_nomes: bool = True
    tamanho_minimo_nome: int = 4
    termos_invalidos_exatos: List[str] = None
    termos_invalidos_regex: List[str] = None
    

    # ========================================================================
    # VARIABLE NAME MAPPINGS
    # ========================================================================
    # Lists of possible column names for each standardized variable.
    # The system will search for the first matching column name in source files.
    # Add variations found in your datasets to these lists below.

    def __post_init__(self):
        # Possible names for variables
        if self.possible_names_nome is None:
            self.possible_names_nome = [
                "nome", "nome_paciente", "nm_pessoa", "nome_completo", "Paciente.Nome1","NOM_PESSOA","NO_PESSOA",
                "NOME", "individuo", "NM_PACIENTE", "no_nome_paciente", "NM_PACIENT","NOME_VITIMA_RAT","Envolvido_Nome", "NO_GESTANTE"
            ]
        
        if self.possible_names_nome_mae is None:
            self.possible_names_nome_mae = [
                "nome_mae", "nm_mae", "nome_genitora", "mae", "no_nome_mae", "nomemae","NOM_COMPLETO_MAE_PESSOA","NO_COMPLETO_MAE_PESSOA",
                "NOME_MAE", "Paciente.Nome.Mae1", "NM_MAE_PACIENTE", "NOMEMAE", "NM_MAE_PAC","MAE"
            ]
        
        if self.possible_names_nome_pai is None:
            self.possible_names_nome_pai = [
                "nome_pai", "nm_pai", "pai", "NOME_PAI","NOM_COMPLETO_PAI_PESSOA", "NO_COMPLETO_PAI_PESSOA",
                "Paciente.Nome.Pai1", "NM_PAI_PACIENTE", "NOMEPAI", "PAI"
            ]
        
        if self.possible_names_cod_municipio is None:
            self.possible_names_cod_municipio = [
                "cod_municipio", "codigo_municipio", "cd_municipio", "ID_MUNICIP","COD_IBGE_MUNIC_NASC_PESSOA", "CO_IBGE_MUNIC_NASC_PESSOA",
                "cod_mun", "codmun", "CODMUN", "municipio_ibge", "Paciente.Municipio.Ibge",
                "CD_MUN", "CODMUNRES", "codmunres","codigo_ibge","Municipio_Fato","CO_MUNICIPIO_IBGE_GESTANTE"
            ]
        
        if self.possible_names_data_nascimento is None:
            self.possible_names_data_nascimento = [
                "data_nascimento", "dt_nascimento", "data_nasc",
                "dtnasc", "DTNASC", "DT_NASC", "Paciente.Data.Nascimento",
                "DATA_NASCIMENTO","DT_NASCIMENTO","DTA_NASC_PESSOA", "DT_NASC_PESSOA"
            ]
        
        if self.possible_names_sexo is None:
            self.possible_names_sexo = [
                "sexo", "sx", "genero", "sex", "SEXO", "tp_sexo",
                "Paciente.Sexo", "CS_SEXO","Envolvido_Sexo","COD_SEXO_PESSOA", "CO_SEXO_PESSOA"
            ]

        if self.possible_names_idade is None:
            self.possible_names_idade = [
                "idade", "NU_IDADE", "nu_idade", "IDADE", "idade_anos",
                "NU_IDADE_N", "nu_idade_n", "IDADE_ANOS", "ID_IDADE",
                "idade_paciente", "IDADE_PACIENTE","Envolvido_Idade"
            ]

        if self.possible_names_data_notificacao is None:
            self.possible_names_data_notificacao = [
                "DT_NOTIFIC", "dt_notificacao", "data_notificacao",
                "DT_SIN_PRI", "DATA_NOTIFICACAO", "dt_notific",
                "data_notific", "DT_NOTIFICACAO", "dt_sin_pri",
                "data_sintomas", "DT_SINTOMAS", "DT_NOTIFICA", "DT_NOTIFIC"
    ]
        
        # ====================================================================
        # SINASC VARIABLE NAMES (fixed, not user-configurable)
        # ====================================================================
        self.sinasc_vars = {
            'nomemae': ['NOMEMAE', 'nomemae', 'NOME_MAE', 'nome_mae', 'NM_MAE_PACIENTE', 'NM_MAE_PAC'],
            'dtnascmae': ['DTNASCMAE', 'dtnascmae', 'DT_NASC_MAE', 'dt_nasc_mae', 'DTNASC_MAE'],
            'nomernasc': ['NOMERNASC', 'nomernasc', 'NOME_RN', 'nome_rn', 'NM_RN', 'NOME_RECEM_NASCIDO'],
            'dtnasc': ['DTNASC', 'dtnasc', 'DT_NASC', 'dt_nasc', 'DATA_NASCIMENTO', 'data_nascimento'],
            'nomepai': ['NOMEPAI', 'nomepai', 'NOME_PAI', 'nome_pai', 'NM_PAI_PACIENTE'],
            'codmunres': ['CODMUNRES', 'codmunres', 'COD_MUN_RES', 'cod_mun_res', 'CODMUN_RES'],
            'sexo': ['SEXO', 'sexo', 'CS_SEXO', 'tp_sexo'],
        }

        # ====================================================================
        # DEFAULT INVALID TERMS FOR NAME CLEANING
        # ====================================================================
        # Comprehensive list of terms that indicate missing/invalid data.
        # Based on analysis of Brazilian health databases (DATASUS systems).
        if self.termos_invalidos_exatos is None:
            self.termos_invalidos_exatos = [
                'sem informacao', 'ni', 'nao tem', 'sem informaaao', 'sem inf', 'sem nome',
                'ignorado', 'ignorada', 'n inf', 'nao informado', 'desconhece', 'sem doc',
                'sem documento', 'desconhecida', 'desconhecido', 'ignordo', 'ignorda',
                'falecida', 'falecido', 'ignrado', 'nao consta', 'nao', 'nan',
                'no consta', 'rua c', 'rua', 'casa', 'dddd', 'ignora', 'brasileiro',
                'brasileira', 'ru a', 'hospital', 'apartamento', 'ignoradao',
                'sem informao', 'nao inf', 'sem id', 'sem identidade', 'nao imformado',
                'sem registro', 'nao informada', 'nao se sabe', 'desconhecidoa',
                'nc', 'na', 'nd', 'si', 'sn', 'snome', 'sinformacao', 'informacao ausente',
                'info ausente', 'nao ha', 'nae', 'n sei', 'nao sei', 'sem mae', 'smae',
                'ausente', 'nao possui', 'nao possui mae', 'sem info',
                'desc', 'nao identificado', 'nome ignorado', 'nome desconhecido',
                'nome nao informado', 'n sabe', 'n consta', 'nao ha registro',
                'sem identificacao', 'sid', 'ilegivel', 'nome ilegivel', 'incompleto',
                'sem sobrenome', 'mae desconhecida', 'nao declarado', 'sem dados',
                'sdados', 'prejudicado', 'prej', 'em branco', 'nao disponivel',
                'nao aplicavel', 'ignorar', 'anonimo', 'anonima', 'indefinido', 'indefinida',
                'indeterminado', 'indeterminada', 'inexistente', 'nao existe', 'sem registro civil',
                'nao consta registro', 'sem mae conhecida', 'ignora se', 'sem certidao',
                'mae falecida', 'mae adotiva', 'sem registro oficial',
                'nao se aplica', 'sem documentos', 'estudante'
            ]
        
        # ====================================================================
        # DEFAULT REGEX PATTERNS FOR INVALID NAMES
        # ====================================================================
        # Regular expressions to catch invalid patterns:
        # - Repetitive characters (AAAA, XXXX, 1111, etc.)
        # - Very short entries with numbers
        # - Single letters repeated
        # - Common address patterns
        if self.termos_invalidos_regex is None:
            self.termos_invalidos_regex = [
                'rua', 'quadra', 'avenida', 'estrada', 'travessa', 'logradouro', 'conj',
                'alameda', 'praca', 'ladeira', 'largo', 'rodovia', 'lote',
                'condominio', 'cond', 'passagem', 'vila', 'conjunto',
                'esposa', 'mae', 'pai', 'esposo', 'rn',
                'fm', 'rm', 'avo', 'lactante', 'lactente', 'gemelar',
                'nati', 'nm', 'ignorado', 'famoso', 'conhecido',
                'vulgo', 'vestido', 'roupa', 'calca', 'camiseta',
                'bermuda', 'saia', 'identificado', 'identificacao',
                'marido', 'mulher',
                'sobrinho', 'sobrinha', 'cunhado', 'cunhada', 'sogro', 'sogra', 'parente',
                'genitora', 'genitor', 'padrasto', 'madrasta', 'enteado', 'enteada', 'bebe', 'crianca',
                'paciente', 'enfermo', 'gestante', 'puerpera', 'internado', 'internada', 'obito',
                'falecimento', 'doente', 'enfermaria', 'leito', 'diagnostico', 'emergencia',
                'parto', 'cesariana', 'cesarea', 'forceps', 'natimorto', 'natimorta', 'feto',
                'prematuro', 'prematura', 'recem', 'nascido', 'nascida', 'recemnascido',
                'hospital', 'posto', 'clinica', 'ambulatorio', 'ubs', 'upa', 'maternidade',
                'domicilio', 'residencia', 'moradia', 'predio', 'apartamento', 'andar', 'quarto',
                'desconhecida', 'desconhecido', 'anonima', 'anonimo', 'indigente', 'indigena',
                'estrangeiro', 'estrangeira', 'viajante', 'pendente', 'provisorio', 'provisoria',
                'temporario', 'temporaria', 'ausente', 'incognito', 'incognita', 'apelido',
                'citada', 'citado', 'mencionada', 'mencionado', 'referida', 'referido'
            ]


def converter_excel_csv(data_folder = UserConfig.data_folder): 
    ### Loop para ler todos os arquivos .xlsx e .xls na pasta especificada e convertê-los para .csv
    for file in os.listdir(data_folder):
        if file.endswith('.xlsx') or file.endswith('.xls'):
            # Lê o arquivo Excel
            excel_file = pd.ExcelFile(os.path.join(data_folder, file))
            
            # Loop para cada planilha no arquivo Excel
            for sheet_name in excel_file.sheet_names:
                # Lê a planilha em um DataFrame
                df = pd.read_excel(excel_file, sheet_name=sheet_name)
                
                # Define o nome do arquivo CSV de saída
                csv_file_name = f"{os.path.splitext(file)[0]}_{sheet_name}.csv"
                csv_file_path = os.path.join(data_folder, csv_file_name)
                
                # Salva o DataFrame como CSV
                df.to_csv(csv_file_path, index=False)
                print(f"Arquivo convertido: {csv_file_path}")
                excel_file.close()
    ### mover todos os arquivos xls e xlsx para uma pasta chamada "originais_excel" dentro da pasta especificada
    originais_excel_folder = os.path.join(data_folder, "originais_excel")   
    os.makedirs(originais_excel_folder, exist_ok=True)
    ### mover os arquivos xls e xlsx para a pasta "originais_excel"
    for file in os.listdir(data_folder):    
        if file.endswith('.xlsx') or file.endswith('.xls'):
           
            os.rename(os.path.join(data_folder, file), os.path.join(originais_excel_folder, file))
            print(f"Arquivo movido para originais_excel: {file}")

@dataclass
class VariableMapping:
    nome: str
    codigo_municipio: Optional[str] = None
    nome_mae: Optional[str] = None
    nome_pai: Optional[str] = None
    data_nascimento: Optional[str] = None
    sexo: Optional[str] = None
    idade: Optional[str] = None
    data_notificacao: Optional[str] = None
    tem_idade_sem_data_nasc: bool = False

def detect_sinasc_in_files(file_names: List[str]) -> Tuple[bool, Optional[str]]:
    """
    Verifica se algum arquivo contém 'SINASC' no nome.
    Retorna (sinasc_detectado, nome_do_arquivo_sinasc).
    """
    for name in file_names:
        if 'SINASC' in name.upper():
            return True, name
    return False, None

def validate_sinasc_config(user_config: UserConfig, sinasc_detectado: bool, sinasc_file: str):
    """
    Valida configuração SINASC. Chamado após detecção de arquivos.
    Se SINASC detectado, verifica que mae/filho não são ambos True ou ambos False.
    """
    if not sinasc_detectado:
        return  # Não é SINASC, ignora flags
    
    print(f"\n  ℹ SINASC DETECTED in file: {sinasc_file}")
    
    if user_config.sinasc_mae and user_config.sinasc_filho:
        raise ValueError(
            f"SINASC CONFIGURATION ERROR:\n"
            f"  Both sinasc_mae=True and sinasc_filho=True.\n"
            f"  You must choose ONE: either deduplicate mothers OR children.\n"
            f"  Set sinasc_mae=True/sinasc_filho=False to deduplicate mothers.\n"
            f"  Set sinasc_mae=False/sinasc_filho=True to deduplicate children."
        )
    
    if not user_config.sinasc_mae and not user_config.sinasc_filho:
        raise ValueError(
            f"SINASC CONFIGURATION ERROR:\n"
            f"  Both sinasc_mae=False and sinasc_filho=False.\n"
            f"  SINASC database detected ({sinasc_file}) but no deduplication target defined.\n"
            f"  Set sinasc_mae=True to deduplicate mothers.\n"
            f"  Set sinasc_filho=True to deduplicate children."
        )
    
    if user_config.sinasc_mae:
        print(f"  Mode: MOTHER deduplication (NOMEMAE → nome_std, DTNASCMAE → data_nascimento_std)")
    else:
        print(f"  Mode: CHILD deduplication (NOMEMAE → nome_std, DTNASC → data_nascimento_std, NOMEPAI → support)")

def detect_sinasc_variables(con: duckdb.DuckDBPyConnection, table_name: str,
                             user_config: UserConfig, file_name: str,
                             is_mae: bool) -> VariableMapping:
    """
    Detecta variáveis para base SINASC usando mapeamento específico.
    Retorna VariableMapping com a promoção de variáveis já aplicada.
    """
    print(f"  ℹ SINASC mode: detecting SINASC-specific variables...")
    
    if is_mae:
        # MAE: NOMEMAE → nome_std, DTNASCMAE → data_nascimento_std
        col_nome = detect_column(con, table_name, user_config.sinasc_vars['nomemae'])
        if not col_nome:
            raise ValueError(
                f"SINASC MAE mode: Column 'NOMEMAE' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['nomemae']}"
            )
        
        col_data_nasc = detect_column(con, table_name, user_config.sinasc_vars['dtnascmae'])
        if not col_data_nasc:
            raise ValueError(
                f"SINASC MAE mode: Column 'DTNASCMAE' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['dtnascmae']}"
            )
        
        col_cod_mun = detect_column(con, table_name, user_config.sinasc_vars['codmunres'])
        if not col_cod_mun:
            raise ValueError(
                f"SINASC MAE mode: Column 'CODMUNRES' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['codmunres']}"
            )
        
        print(f"    NOMEMAE ({col_nome}) → nome_std")
        print(f"    DTNASCMAE ({col_data_nasc}) → data_nascimento_std")
        print(f"    CODMUNRES ({col_cod_mun}) → codigo_municipio_std")
        print(f"    sexo → forced 'F'")
        print(f"    nome_mae_std → NULL (no grandmother info)")
        print(f"    nome_pai_std → NULL")
        
        return VariableMapping(
            nome=col_nome,
            codigo_municipio=col_cod_mun,
            nome_mae=None,
            nome_pai=None,
            data_nascimento=col_data_nasc,
            sexo=None  # Will be forced to 'F' in standardization SQL
        )
    
    else:
        # FILHO: NOMEMAE → nome_std, DTNASC → data, NOMEPAI → nome_mae_std (support)
        col_nomemae = detect_column(con, table_name, user_config.sinasc_vars['nomemae'])
        if not col_nomemae:
            raise ValueError(
                f"SINASC FILHO mode: Column 'NOMEMAE' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['nomemae']}"
            )
        
        col_dtnasc = detect_column(con, table_name, user_config.sinasc_vars['dtnasc'])
        if not col_dtnasc:
            raise ValueError(
                f"SINASC FILHO mode: Column 'DTNASC' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['dtnasc']}"
            )
        
        col_nomepai = detect_column(con, table_name, user_config.sinasc_vars['nomepai'])
        if not col_nomepai:
            print(f"  ⚠ WARNING: NOMEPAI not found in {file_name} - will use NULL as support")
        
        col_cod_mun = detect_column(con, table_name, user_config.sinasc_vars['codmunres'])
        if not col_cod_mun:
            raise ValueError(
                f"SINASC FILHO mode: Column 'CODMUNRES' not found in {file_name}\n"
                f"  Searched for: {user_config.sinasc_vars['codmunres']}"
            )
        
        col_sexo = detect_column(con, table_name, user_config.sinasc_vars['sexo'])
        
        print(f"    NOMEMAE ({col_nomemae}) → nome_std (promoted)")
        print(f"    DTNASC ({col_dtnasc}) → data_nascimento_std")
        print(f"    NOMEPAI ({col_nomepai}) → nome_mae_std (support role)")
        print(f"    CODMUNRES ({col_cod_mun}) → codigo_municipio_std")
        if col_sexo:
            print(f"    SEXO ({col_sexo}) → sexo_std")
        print(f"    nome_pai_std → NULL")
        print(f"    NOMERNASC → ignored (not used in deduplication)")
        
        return VariableMapping(
            nome=col_nomemae,          # NOMEMAE promoted to nome
            codigo_municipio=col_cod_mun,
            nome_mae=col_nomepai,      # NOMEPAI in support role (mapped to nome_mae_std)
            nome_pai=None,             # No second support
            data_nascimento=col_dtnasc,
            sexo=col_sexo
        )

def get_user_config() -> UserConfig:
    return UserConfig()

#Identifica a base do sim pelo nome
def get_normalized_source(file_name: str) -> str:
    """
    Normaliza o nome da fonte.
    Se contém 'SIM', retorna 'SIM' (base de referência, não deduplica entre si).
    Se contém 'SINASC', retorna 'SINASC'.
    Caso contrário, retorna o nome original do arquivo.
    """
    upper_name = file_name.upper()

    # Verificar SINASC primeiro para evitar que "SINASC" seja capturado por "SIM" 
    # (não acontece, mas por segurança)
    if "SINASC" in upper_name:
        return "SINASC"
    if "CADUNICO" in upper_name:
        return "CADUNICO"
    if "SIM" in upper_name:
        return "SIM"
    return file_name

# ========================================================================================
# FILE FORMAT DETECTION
# ========================================================================================

# Functions to identify file types and detect character encodings automatically.
# Critical for handling diverse data sources with varying formats and encodings
# commonly found in Brazilian health databases (DATASUS, SINAN, SIM, etc.).

class FileFormat(Enum):
    CSV = "csv"
    TXT = "txt"
    PARQUET = "parquet"
    DBF = "dbf"
    DBC = "dbc"
    XLS = "xls"
    XLSX = "xlsx"
    UNKNOWN = "unknown"

@dataclass
class FileProfile:
    path: Path
    format: FileFormat
    encoding: Optional[str] = None
    delimiter: Optional[str] = None
    has_header: bool = True
    quote_char: Optional[str] = None

def detect_encoding(file_path: Path, sample_size: int = 100000) -> str:
    """Detect character encoding of a text file using statistical analysis.

    Uses chardet library to analyze byte patterns and identify the most likely
    encoding. Essential for correctly reading CSV and DBF files.
    
    The function reads a sample of the file (enough for reliable detection)
    rather than the entire file for performance.
    
    Process:
        1. Read up to 100KB of file for analysis (representative sample)
        2. chardet analyzes byte patterns and frequency distributions
        3. Returns encoding with highest confidence score
        4. Falls back to latin1 if confidence < 0.7 or detection fails

    Note: Binary formats (Parquet, Feather, Excel) don't need encoding detection
          as they have built-in encoding specifications.
    
    """
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(sample_size)
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            confidence = result['confidence']
            
            if confidence < 0.7 or encoding is None:
                for enc in ['utf-8', 'latin-1', 'cp1252', 'cp850', 'iso-8859-1']:
                    try:
                        with open(file_path, 'r', encoding=enc) as test_file:
                            test_file.read(1000)
                        return enc
                    except:
                        continue
            
            return encoding if encoding else 'utf-8'
    except Exception as e:
        return 'utf-8'

def map_encoding_to_duckdb(chardet_encoding: str) -> str:
    """Map chardet encoding names to DuckDB encoding names"""
    encoding_map = {
        'ISO-8859-1': 'cp850',
        'latin-1': 'cp850',
        'latin1': 'cp850',
        'cp850': 'cp850',
        'iso-8859-1': 'cp850',
        'windows-1252': 'CP1252',
        'Windows-1252': 'CP1252',
        'cp1252': 'CP1252',
        'utf-8': 'UTF-8',
        'UTF-8': 'UTF-8',
        'ascii': 'UTF-8',
        'ASCII': 'UTF-8',
    }
    return encoding_map.get(chardet_encoding, 'UTF-8')  # ALTERADO: default UTF8 -> UTF-8

def calculate_text_quality_score(text: str) -> float:
    """Calculate text quality score"""
    if not text:
        return 0.0
    
    valid_chars = 'áéíóúàèìòùâêîôûãõçÁÉÍÓÚÀÈÌÒÙÂÊÎÔÛÃÕÇ'
    problem_chars = 'ÃÂ©Ã¡Ã©Ã­Ã³ÃºÃ£ÃµÃ§'
    problem_patterns = ['Ã©', 'Ã¡', 'Ã­', 'Ã³', 'Ãº', 'Ã£', 'Ãµ', 'Ã§', 'Ã ', 'Ãª', 'Ã´']
    
    score = 0.0
    for char in valid_chars:
        score += text.count(char)
    for char in problem_chars:
        score -= text.count(char) * 10
    for pattern in problem_patterns:
        score -= text.count(pattern) * 50
    
    return score / max(len(text), 1)

def find_column_match(column_names: List[str], possible_names: List[str]) -> Optional[str]:
    """Find matching column name from list"""
    column_names_lower = [col.lower() for col in column_names]
    
    for possible_name in possible_names:
        possible_lower = possible_name.lower()
        if possible_lower in column_names_lower:
            idx = column_names_lower.index(possible_lower)
            return column_names[idx]
    
    return None

# ========================================================================================
# SAFE DBF FIELD PARSER
# ========================================================================================
# Campos numéricos corrompidos em DBFs do DATASUS (ex: bytes 0xAA em campo tipo N)
# causam "could not convert string to float" no dbfread padrão.
# Este parser customizado captura o erro e retorna o valor como string bruta,
# permitindo que o pipeline continue — os campos relevantes (nome, data) não são afetados.

from dbfread import FieldParser as _FieldParser

class SafeFieldParser(_FieldParser):
    def parseN(self, field, data):
        try:
            return super().parseN(field, data)
        except (ValueError, TypeError):
            return data.decode(self.encoding, errors='ignore').strip()
    
    def parseF(self, field, data):
        try:
            return super().parseF(field, data)
        except (ValueError, TypeError):
            return data.decode(self.encoding, errors='ignore').strip()

def detect_encoding_for_dbf(file_path: Path, user_config: UserConfig) -> str:
    """Detect encoding for DBF files"""
    from dbfread import DBF
    
    encodings_to_test = ['cp850', 'cp1252', 'latin-1', 'iso-8859-1', 'utf-8']
    
    best_encoding = 'latin-1'
    best_score = float('-inf')
    
    print(f"  Testing encodings for DBF file...")
    
    for encoding in encodings_to_test:
        try:
            table = DBF(str(file_path), encoding=encoding, ignore_missing_memofile=True, parserclass=SafeFieldParser)
            records = list(table)
            
            if not records:
                continue
            
            column_names = list(records[0].keys())
            
            nome_col = find_column_match(column_names, user_config.possible_names_nome)
            nome_mae_col = find_column_match(column_names, user_config.possible_names_nome_mae)
            nome_pai_col = find_column_match(column_names, user_config.possible_names_nome_pai)
            
            relevant_cols = []
            if nome_col:
                relevant_cols.append(nome_col)
            if nome_mae_col:
                relevant_cols.append(nome_mae_col)
            if nome_pai_col:
                relevant_cols.append(nome_pai_col)
            
            if not relevant_cols:
                relevant_cols = column_names
            
            total_records = len(records)
            sample_size = max(10, min(1000, int(total_records * 0.1)))
            sample_records = records[:sample_size]
            
            text_sample = []
            for record in sample_records:
                for col in relevant_cols:
                    value = record.get(col)
                    if value and isinstance(value, str):
                        text_sample.append(value)
            
            combined_text = ' '.join(text_sample)
            score = calculate_text_quality_score(combined_text)
            
            print(f"    {encoding:12s} -> score: {score:8.2f} (sample: {len(combined_text)} chars, {len(text_sample)} names)")
            
            if score > best_score:
                best_score = score
                best_encoding = encoding
                
        except Exception as e:
            print(f"    {encoding:12s} -> failed: {str(e)[:50]}")
            continue
    
    print(f"  ✓ Best encoding detected: {best_encoding} (score: {best_score:.2f})")
    return best_encoding

def detect_delimiter(file_path: Path, encoding: str) -> Tuple[str, str]:
    """Detect delimiter and quote character"""
    try:
        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            sample = f.read(10000)
            
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=',;\t|')
            
            delimiter = dialect.delimiter
            quote_char = dialect.quotechar
            
            return delimiter, quote_char
    except Exception as e:
        return ',', '"'

def profile_file(file_path: Path, user_config: Optional[UserConfig] = None) -> FileProfile:
    """Profile a file to determine format and characteristics"""
    
    extension = file_path.suffix.lower()
    format_map = {
        '.csv': FileFormat.CSV,
        '.txt': FileFormat.TXT,
        '.parquet': FileFormat.PARQUET,
        '.dbf': FileFormat.DBF,
        '.dbc': FileFormat.DBC,
        '.xls': FileFormat.XLS,
        '.xlsx': FileFormat.XLSX
    }
    
    file_format = format_map.get(extension, FileFormat.UNKNOWN)
    
    profile = FileProfile(
        path=file_path,
        format=file_format
    )
    
    if file_format in [FileFormat.CSV, FileFormat.TXT]:
        profile.encoding = detect_encoding(file_path)
        profile.delimiter, profile.quote_char = detect_delimiter(file_path, profile.encoding)
    
    elif file_format in [FileFormat.DBF, FileFormat.DBC] and user_config:
        profile.encoding = detect_encoding_for_dbf(file_path, user_config)
    
    elif file_format in [FileFormat.DBF, FileFormat.DBC] and not user_config:
        profile.encoding = 'latin-1'
    
    return profile

# ========================================================================================
# FILE READERS FOR DIFFERENT FORMATS
# ========================================================================================

# Format-specific reading functions that handle the unique characteristics of each
# file type. Each reader encapsulates the pandas I/O logic, parameters, and error
# handling appropriate for its format. This modular approach allows easy addition
# of new formats and centralized handling of format-specific formats.

def read_file_to_duckdb(profile: FileProfile, con: duckdb.DuckDBPyConnection, 
                        table_name: str) -> int:
    """Read file into DuckDB table based on format"""
    
    try:
        if profile.format == FileFormat.PARQUET:
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT * FROM read_parquet('{profile.path}')
            """)
        
        elif profile.format in [FileFormat.CSV, FileFormat.TXT]:
            duckdb_encoding = map_encoding_to_duckdb(profile.encoding)
            con.execute(f"""
                CREATE OR REPLACE TABLE {table_name} AS 
                SELECT * FROM read_csv(
                    '{profile.path}',
                    delim='{profile.delimiter}',
                    quote='{profile.quote_char}',
                    header=true,
                    encoding='{duckdb_encoding}',
                    ignore_errors=false,
                    all_varchar=true
                )
            """)
        
        elif profile.format == FileFormat.DBF:
            try:
                from dbfread import DBF

                table = DBF(str(profile.path), encoding=profile.encoding, ignore_missing_memofile=True, parserclass=SafeFieldParser)
                records = list(table)

                if not records:
                    raise ValueError("DBF file is empty")

                fieldnames = list(records[0].keys())
                df = pl.DataFrame({
                    col: [str(r.get(col, '')) if r.get(col) is not None else None for r in records]
                    for col in fieldnames
                })

                con.register('temp_polars_df', df)
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_polars_df")
                con.unregister('temp_polars_df')
                
            except ImportError:
                raise ImportError("DBF support requires 'dbfread' library. Install with: pip install dbfread")
        
        elif profile.format == FileFormat.DBC:
            try:
                try:
                    from pysus.utilities.readdbc import read_dbc
                    import tempfile
                    
                    df_dbc = read_dbc(str(profile.path), encoding=profile.encoding)
                    con.register('temp_dbc_df', df_dbc)
                    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_dbc_df")
                    con.unregister('temp_dbc_df')
                    
                except ImportError:
                    import subprocess
                    import tempfile
                    
                    with tempfile.NamedTemporaryFile(suffix='.dbf', delete=False) as tmp_dbf:
                        tmp_dbf_path = tmp_dbf.name
                    
                    try:
                        subprocess.run(['blast-dbf', str(profile.path), tmp_dbf_path], 
                                     check=True, capture_output=True)
                    except (subprocess.CalledProcessError, FileNotFoundError):
                        raise ImportError(
                            "DBC decompression requires 'pysus' library or 'blast-dbf' tool.\n"
                            "Install with: pip install pysus\n"
                            "Or install blast-dbf system package"
                        )
                    
                    from dbfread import DBF
                    table = DBF(tmp_dbf_path, encoding=profile.encoding, ignore_missing_memofile=True)
                    records = list(table)
                    
                    if not records:
                        raise ValueError("DBC/DBF file is empty")
                    
                    fieldnames = list(records[0].keys())
                    df = pl.DataFrame({
                        col: [str(r.get(col, '')) if r.get(col) is not None else None for r in records]
                        for col in fieldnames
                    })

                    con.register('temp_polars_df', df)
                    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_polars_df")
                    con.unregister('temp_polars_df')

                    Path(tmp_dbf_path).unlink(missing_ok=True)
                    
            except Exception as e:
                raise RuntimeError(f"Failed to process DBC file: {e}")
        
        elif profile.format in [FileFormat.XLS, FileFormat.XLSX]:
            try:
                import pandas as pd
                
                df = pd.read_excel(profile.path, dtype=str)
                con.register('temp_excel_df', df)
                con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_excel_df")
                con.unregister('temp_excel_df')
                
            except ImportError:
                raise ImportError("Excel support requires 'pandas' and 'openpyxl'. Install with: pip install pandas openpyxl")
        
        else:
            raise ValueError(f"Unsupported file format: {profile.format}")
        
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  Loaded {count:,} records")
        
        return count
    
    except Exception as e:
        raise RuntimeError(f"Failed to read file: {str(e)}")

# ========================================================================================
# COLUMN DETECTION AND MAPPING
# ========================================================================================

# Functions to identify and map columns from source files to standardized variable names.
# Handles case-insensitive matching and multiple naming conventions across different
# data sources. Critical for creating a unified schema from heterogeneous input files.

def escape_column_name(col_name: str) -> str:
    """Escape column names with special characters"""
    if '.' in col_name or ' ' in col_name or '-' in col_name:
        return f'"{col_name}"'
    return col_name

def detect_column(con: duckdb.DuckDBPyConnection, table_name: str, 
                 possible_names: List[str]) -> Optional[str]:
    """Detect column by trying possible names"""
    columns = con.execute(f"DESCRIBE {table_name}").fetchall()
    column_names = [col[0] for col in columns]
    column_names_lower = [col.lower() for col in column_names]
    
    for possible_name in possible_names:
        possible_lower = possible_name.lower()
        if possible_lower in column_names_lower:
            idx = column_names_lower.index(possible_lower)
            return escape_column_name(column_names[idx])
    
    return None



def detect_variables(con: duckdb.DuckDBPyConnection, table_name: str, 
                    user_config: UserConfig, file_name: str) -> VariableMapping:
    """Detect required variables in the table"""
    
    columns = con.execute(f"DESCRIBE {table_name}").fetchall()
    available_columns = [col[0] for col in columns]
    
    col_nome = detect_column(con, table_name, user_config.possible_names_nome)
    if not col_nome:
        raise ValueError(
            f"Column 'nome' not found in {file_name}\n"
            f"  Searched for: {user_config.possible_names_nome}\n"
            f"  Available columns: {available_columns}"
        )
    
    col_nome_mae = detect_column(con, table_name, user_config.possible_names_nome_mae)
    if not col_nome_mae:
        print(f"  ⚠ WARNING: Column 'nome_mae' not found in {file_name} - will use NULL")

    col_nome_pai = detect_column(con, table_name, user_config.possible_names_nome_pai)
    if not col_nome_pai:
        print(f"  ⚠ WARNING: Column 'nome_pai' not found in {file_name} - will use NULL")
       
    col_data_nasc = detect_column(con, table_name, user_config.possible_names_data_nascimento)
    col_idade = None
    col_data_notific = None
    tem_idade_sem_data_nasc = False

    if not col_data_nasc:
        # Não tem data de nascimento — procura idade
        col_idade = detect_column(con, table_name, user_config.possible_names_idade)
        
        if not col_idade:
            raise ValueError(
                f"Neither 'data_nascimento' nor 'idade' found in {file_name}\n"
                f"  Searched for data_nascimento: {user_config.possible_names_data_nascimento}\n"
                f"  Searched for idade: {user_config.possible_names_idade}\n"
                f"  Available columns: {available_columns}"
            )
        
        # Tem idade — procura data de notificação
        col_data_notific = detect_column(con, table_name, user_config.possible_names_data_notificacao)
        
        if not col_data_notific:
            raise ValueError(
                f"Column 'idade' found but 'data_notificacao' not found in {file_name}\n"
                f"  Base has 'idade' ({col_idade}) but no reference date for year calculation.\n"
                f"  Searched for data_notificacao: {user_config.possible_names_data_notificacao}\n"
                f"  Available columns: {available_columns}"
            )
        
        print(f"  ℹ Base {file_name}: No 'data_nascimento' found. Using 'idade' ({col_idade}) + 'data_notificacao' ({col_data_notific})")
        tem_idade_sem_data_nasc = True
    
    col_cod_mun = detect_column(con, table_name, user_config.possible_names_cod_municipio)
    if not col_cod_mun:
        print(f"  ⚠ WARNING: Column 'codigo_municipio' not found in {file_name} - will use NULL")
    
    col_sexo = detect_column(con, table_name, user_config.possible_names_sexo)
    
    return VariableMapping(
        nome=col_nome,
        nome_mae=col_nome_mae,
        nome_pai=col_nome_pai,
        data_nascimento=col_data_nasc,
        codigo_municipio=col_cod_mun,
        sexo=col_sexo,
        idade=col_idade,
        data_notificacao=col_data_notific,
        tem_idade_sem_data_nasc=tem_idade_sem_data_nasc
    )

# ========================================================================================
# DATE FORMAT DETECTION AND STANDARDIZATION
# ========================================================================================
# Automatic detection and conversion of date formats to standardized DDMMYYYY string format.
# 
# Process:
# 1. Samples dates from the column to detect predominant format
# 2. Uses validate_date() to test if parsed dates are logically valid
# 3. Selects format with highest number of valid parses
# 4. Generates SQL to convert ALL dates to DDMMYYYY format (as strings)
#
# IMPORTANT LIMITATION:
# - Format detection uses validation to IDENTIFY the format
# - BUT the actual conversion does NOT reject invalid dates
# - Invalid dates (e.g., Feb 30, Month 13) are converted and KEPT in the data
# - They remain as invalid string values (e.g., "30022023")
# - No warnings or flags are generated for invalid dates
# - Invalid dates will still be compared during deduplication (as strings)
#
# This means: If source data has "30/02/2023", it becomes "30022023" and stays in the dataset.

class DateFormat(Enum):
    DDMMYYYY = "DDMMYYYY"
    DDMMYY = "DDMMYY"
    MMDDYYYY = "MMDDYYYY"
    MMDDYY = "MMDDYY"
    YYYYMMDD = "YYYYMMDD"
    YYMMDD = "YYMMDD"
    YYYYDDMM = "YYYYDDMM"
    YYDDMM = "YYDDMM"

def validate_date(day: int, month: int, year: int) -> bool:
    if year < 1900 or year > datetime.now().year:
        return False
    if month < 1 or month > 12:
        return False
    if day < 1 or day > 31:
        return False
    
    if month == 2:
        is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
        return day <= (29 if is_leap else 28)
    elif month in [4, 6, 9, 11]:
        return day <= 30
    else:
        return day <= 31

def fix_invalid_year(year: int) -> int:
    """
    Corrige anos inválidos (fora da faixa 1900..ano_atual).
    
    Se o ano está dentro da faixa válida, retorna inalterado.
    Se está fora, pega os 2 últimos dígitos e aplica:
      - yy <= ano_atual_2dig -> 20xx
      - yy > ano_atual_2dig  -> 19xx
    
    Exemplos (assumindo ano atual = 2026):
      0993  -> fora da faixa -> 93 > 26 -> 1993
      0018  -> fora da faixa -> 18 <= 26 -> 2018
      1001  -> fora da faixa -> 01 <= 26 -> 2001
      2907  -> fora da faixa -> 07 <= 26 -> 2007
      2093  -> fora da faixa -> 93 > 26 -> 1993
      4584  -> fora da faixa -> 84 > 26 -> 1984
      1993  -> dentro da faixa -> 1993 (inalterado)
      2015  -> dentro da faixa -> 2015 (inalterado)
    """
    current_year = datetime.now().year
    
    if 1900 <= year <= current_year:
        return year  # ano válido, não mexe
    
    # Ano fora da faixa: pega últimos 2 dígitos e aplica regra <100 anos
    yy = year % 100
    current_year_2dig = current_year % 100
    return 2000 + yy if yy <= current_year_2dig else 1900 + yy

def try_format_for_detection(clean_date: str, date_format: DateFormat) -> Optional[Tuple[int, int, int]]:
    length = len(clean_date)
    
    try:
        if date_format == DateFormat.DDMMYYYY and length == 8:
            d, m, a = int(clean_date[0:2]), int(clean_date[2:4]), int(clean_date[4:8])
        elif date_format == DateFormat.DDMMYY and length == 6:
            d, m = int(clean_date[0:2]), int(clean_date[2:4])
            yy = int(clean_date[4:6])
            current_year_2dig = datetime.now().year % 100
            a = 2000 + yy if yy <= current_year_2dig else 1900 + yy
        elif date_format == DateFormat.MMDDYYYY and length == 8:
            m, d, a = int(clean_date[0:2]), int(clean_date[2:4]), int(clean_date[4:8])
        elif date_format == DateFormat.MMDDYY and length == 6:
            m, d = int(clean_date[0:2]), int(clean_date[2:4])
            yy = int(clean_date[4:6])
            current_year_2dig = datetime.now().year % 100
            a = 2000 + yy if yy <= current_year_2dig else 1900 + yy
        elif date_format == DateFormat.YYYYMMDD and length == 8:
            a, m, d = int(clean_date[0:4]), int(clean_date[4:6]), int(clean_date[6:8])
        elif date_format == DateFormat.YYMMDD and length == 6:
            yy = int(clean_date[0:2])
            current_year_2dig = datetime.now().year % 100
            a = 2000 + yy if yy <= current_year_2dig else 1900 + yy
            m, d = int(clean_date[2:4]), int(clean_date[4:6])
        elif date_format == DateFormat.YYYYDDMM and length == 8:
            a, d, m = int(clean_date[0:4]), int(clean_date[4:6]), int(clean_date[6:8])
        elif date_format == DateFormat.YYDDMM and length == 6:
            yy = int(clean_date[0:2])
            current_year_2dig = datetime.now().year % 100
            a = 2000 + yy if yy <= current_year_2dig else 1900 + yy
            d, m = int(clean_date[2:4]), int(clean_date[4:6])
        else:
            return None
        
        # Corrige anos com 0 na frente (erro de digitação)
        a = fix_invalid_year(a)
        
        if validate_date(d, m, a):
            return (d, m, a)
        return None
    except:
        return None

def detect_date_format(con: duckdb.DuckDBPyConnection, table_name: str,
                      column_name: str, sample_percent: float = 0.10) -> DateFormat:
    """Detect date format in a column"""
    
    total_records = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    sample_size = max(500, min(int(total_records * sample_percent), total_records))
    
    sample_data = con.execute(f"""
        SELECT {column_name}
        FROM {table_name}
        USING SAMPLE {sample_size}
    """).fetchall()
    
    regex_clean = re.compile(r"[^0-9]")
    counters = {fmt: 0 for fmt in DateFormat}
    total_processed = 0
    
    for (date_str,) in sample_data:
        if date_str is None:
            continue

        date_str_raw = str(date_str).strip().split(' ')[0]
        # Detecta delimitador (qualquer caractere não-numérico) e faz zero-padding
        # Primeiro remove tudo após espaço (hora), depois identifica separador
        delimiter_match = re.search(r'[^0-9]', date_str_raw)
        if delimiter_match:
            delimiter = delimiter_match.group()
            parts = date_str_raw.split(delimiter)
            if len(parts) == 3:
                # Extrai só dígitos de cada parte
                raw_parts = [regex_clean.sub("", p) for p in parts]
                # Identifica qual parte é o ano (a que tem mais dígitos, ou >2)
                # e expande ano de 2 dígitos para 4 se necessário
                expanded_parts = []
                for rp in raw_parts:
                    if len(rp) == 2:
                        expanded_parts.append(rp)  # pode ser dia, mês ou ano curto
                    elif len(rp) == 4:
                        expanded_parts.append(rp)  # ano longo, dia/mês improvável
                    elif len(rp) == 1:
                        expanded_parts.append(rp.zfill(2))  # dia ou mês sem zero
                    else:
                        expanded_parts.append(rp)  # qualquer outro comprimento
                clean_date = ''.join(expanded_parts)
            else:
                clean_date = regex_clean.sub("", date_str_raw)
        else:
            clean_date = regex_clean.sub("", date_str_raw)

        if not clean_date or len(clean_date) < 6 or len(clean_date) > 8:
            continue

        found_valid = False
        for date_format in DateFormat:
            if try_format_for_detection(clean_date, date_format):
                counters[date_format] += 1
                found_valid = True
        
        if found_valid:
            total_processed += 1
    
    if not any(counters.values()):
        raise ValueError("Could not detect any valid date format")
    
    detected_format = max(counters.items(), key=lambda x: x[1])[0]
    
    return detected_format

def detect_data_notificacao_type(con: duckdb.DuckDBPyConnection, table_name: str,
                                  column_name: str, sample_percent: float = 0.10) -> str:
    """
    Detecta o tipo de informação no campo data_notificacao.
    Retorna: '2_digitos', '4_digitos', ou 'data_completa'
    
    Amostra o campo, limpa para ficar só dígitos, e classifica pelo
    comprimento predominante na amostra.
    """
    total_records = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    sample_size = max(100, min(int(total_records * sample_percent), total_records))
    
    sample_data = con.execute(f"""
        SELECT {column_name}
        FROM {table_name}
        WHERE {column_name} IS NOT NULL AND CAST({column_name} AS VARCHAR) != ''
        USING SAMPLE {sample_size}
    """).fetchall()
    
    regex_clean = re.compile(r"[^0-9]")
    contagem = {'2_digitos': 0, '4_digitos': 0, 'data_completa': 0}
    
    for (valor,) in sample_data:
        if valor is None:
            continue
        valor_raw = str(valor).strip().split(' ')[0]
        delimiter_match = re.search(r'[^0-9]', valor_raw)
        if delimiter_match:
            delimiter = delimiter_match.group()
            parts = valor_raw.split(delimiter)
            if len(parts) == 3:
                padded_parts = []
                for p in parts:
                    digits = regex_clean.sub("", p)
                    if len(digits) <= 2:
                        digits = digits.zfill(2)
                    padded_parts.append(digits)
                clean = ''.join(padded_parts)
            else:
                clean = regex_clean.sub("", valor_raw)
        else:
            clean = regex_clean.sub("", valor_raw)
        if not clean:
            continue

        length = len(clean)
        if length == 2:
            contagem['2_digitos'] += 1
        elif length == 4:
            contagem['4_digitos'] += 1
        elif length > 4:
            contagem['data_completa'] += 1
    
    if not any(contagem.values()):
        raise ValueError(f"Could not detect data_notificacao type in column {column_name}")
    
    tipo_detectado = max(contagem.items(), key=lambda x: x[1])[0]
    
    print(f"  Data notificacao type detected: {tipo_detectado}")
    print(f"    Distribution: 2_digitos={contagem['2_digitos']}, 4_digitos={contagem['4_digitos']}, data_completa={contagem['data_completa']}")
    
    return tipo_detectado

def get_ano_notificacao_sql(tipo_data_notific: str, col_data_notific: str, 
                             col_idade: str) -> str:
    """
    Gera SQL para extrair o ano de notificação com base no tipo detectado.
    
    - '4_digitos': usa direto como ano
    - '2_digitos': expande para 4 dígitos usando a idade como referência
    - 'data_completa': trata como data, extrai o ano
    """
    clean_col = f"regexp_replace(CAST({col_data_notific} AS VARCHAR), '[^0-9]', '', 'g')"
    idade_col = f"CAST({col_idade} AS INTEGER)"
    
    if tipo_data_notific == '4_digitos':
        return f"CAST({clean_col} AS INTEGER)"
    
    elif tipo_data_notific == '2_digitos':
        # Se idade < 100, tenta 2000+yy. Valida se (2000+yy)-idade está entre 1900 e ano atual.
        # Se não faz sentido, usa 1900+yy.
        return f"""
            CASE
                WHEN {idade_col} IS NOT NULL AND {idade_col} < 100
                     AND (2000 + CAST({clean_col} AS INTEGER)) - {idade_col} BETWEEN 1900 AND EXTRACT(YEAR FROM CURRENT_DATE)
                THEN 2000 + CAST({clean_col} AS INTEGER)
                ELSE 1900 + CAST({clean_col} AS INTEGER)
            END
        """
    
    else:
        # data_completa: será tratada pelo detect_date_format normal
        # Este caso é resolvido externamente antes de chamar esta função
        # Retorna placeholder — o chamador deve usar get_date_conversion_sql + extração de ano
        return None

def get_date_conversion_sql(format_detected: DateFormat, column_name: str, is_sinasc: bool = False) -> str:
    """Generate SQL for date conversion based on detected format"""
    
    current_year_2dig = datetime.now().year % 100
    # Normaliza datas com delimitador (ex: "1/1/2026") via zero-padding antes de limpar.
    # Se a data contém separador (/ - .) e 3 partes, faz lpad nas duas primeiras (dia/mês).
    raw_col = f"TRIM(string_split(CAST({column_name} AS VARCHAR), ' ')[1])"
    
    # Detecta o primeiro caractere não-numérico como delimitador
    delim_expr = f"regexp_extract({raw_col}, '([^0-9])', 1)"
    
    # Se tem delimitador e 3 partes: extrai dígitos de cada parte, faz zero-padding
    # em partes com 1 dígito, mantém partes com 2+ dígitos como estão
    clean_col = f"""CASE
        WHEN {raw_col} ~ '[^0-9]'
             AND array_length(string_split({raw_col}, {delim_expr}), 1) = 3
        THEN (CASE WHEN length(regexp_replace(string_split({raw_col}, {delim_expr})[1], '[^0-9]', '', 'g')) <= 2
                   THEN lpad(regexp_replace(string_split({raw_col}, {delim_expr})[1], '[^0-9]', '', 'g'), 2, '0')
                   ELSE regexp_replace(string_split({raw_col}, {delim_expr})[1], '[^0-9]', '', 'g')
              END)
          || (CASE WHEN length(regexp_replace(string_split({raw_col}, {delim_expr})[2], '[^0-9]', '', 'g')) <= 2
                   THEN lpad(regexp_replace(string_split({raw_col}, {delim_expr})[2], '[^0-9]', '', 'g'), 2, '0')
                   ELSE regexp_replace(string_split({raw_col}, {delim_expr})[2], '[^0-9]', '', 'g')
              END)
          || (CASE WHEN length(regexp_replace(string_split({raw_col}, {delim_expr})[3], '[^0-9]', '', 'g')) <= 2
                   THEN lpad(regexp_replace(string_split({raw_col}, {delim_expr})[3], '[^0-9]', '', 'g'), 2, '0')
                   ELSE regexp_replace(string_split({raw_col}, {delim_expr})[3], '[^0-9]', '', 'g')
              END)
        ELSE regexp_replace({raw_col}, '[^0-9]', '', 'g')
    END"""
    
    conversions = {
        DateFormat.DDMMYYYY: f"""
            CASE 
                WHEN length({clean_col}) = 8 THEN {clean_col}
                WHEN length({clean_col}) = 7 THEN '0' || {clean_col}
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 1, 2) ||
                    substr({clean_col}, 3, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 5, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 5, 2)
                        ELSE '19' || substr({clean_col}, 5, 2)
                    END
                ELSE {clean_col}
            END
        """,
       
        DateFormat.DDMMYY: f"""
            CASE 
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 1, 2) ||
                    substr({clean_col}, 3, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 5, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 5, 2)
                        ELSE '19' || substr({clean_col}, 5, 2)
                    END
                WHEN length({clean_col}) = 5 THEN
                    '0' || substr({clean_col}, 1, 1) ||
                    substr({clean_col}, 2, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 4, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 4, 2)
                        ELSE '19' || substr({clean_col}, 4, 2)
                    END
                ELSE {clean_col}
            END
        """,

        DateFormat.MMDDYYYY: f"""
            CASE 
                WHEN length({clean_col}) = 8 THEN
                    substr({clean_col}, 3, 2) ||
                    substr({clean_col}, 1, 2) ||
                    substr({clean_col}, 5, 4)
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 3, 2) ||
                    substr({clean_col}, 1, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 5, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 5, 2)
                        ELSE '19' || substr({clean_col}, 5, 2)
                    END
                WHEN length({clean_col}) = 7 THEN '0' || {clean_col}
                ELSE {clean_col}
            END
        """,
        
        DateFormat.MMDDYY: f"""
            CASE 
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 3, 2) ||
                    substr({clean_col}, 1, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 5, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 5, 2)
                        ELSE '19' || substr({clean_col}, 5, 2)
                    END
                WHEN length({clean_col}) = 5 THEN '0' || {clean_col}
                ELSE {clean_col}
            END
        """,
        
        DateFormat.YYYYMMDD: f"""
            CASE 
                WHEN length({clean_col}) = 8 THEN
                    substr({clean_col}, 7, 2) ||
                    substr({clean_col}, 5, 2) ||
                    substr({clean_col}, 1, 4)
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 5, 2) ||
                    substr({clean_col}, 3, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 1, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 1, 2)
                        ELSE '19' || substr({clean_col}, 1, 2)
                    END
                ELSE {clean_col}
            END
        """,
        
        DateFormat.YYMMDD: f"""
            CASE 
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 5, 2) ||
                    substr({clean_col}, 3, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 1, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 1, 2)
                        ELSE '19' || substr({clean_col}, 1, 2)
                    END
                ELSE {clean_col}
            END
        """,
        
        DateFormat.YYYYDDMM: f"""
            CASE 
                WHEN length({clean_col}) = 8 THEN
                    substr({clean_col}, 5, 2) ||
                    substr({clean_col}, 7, 2) ||
                    substr({clean_col}, 1, 4)
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 3, 2) ||
                    substr({clean_col}, 5, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 1, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 1, 2)
                        ELSE '19' || substr({clean_col}, 1, 2)
                    END
                ELSE {clean_col}
            END
        """,
        
        DateFormat.YYDDMM: f"""
            CASE 
                WHEN length({clean_col}) = 6 THEN
                    substr({clean_col}, 3, 2) ||
                    substr({clean_col}, 5, 2) ||
                    CASE 
                        WHEN CAST(substr({clean_col}, 1, 2) AS INTEGER) <= {current_year_2dig}
                            THEN '20' || substr({clean_col}, 1, 2)
                        ELSE '19' || substr({clean_col}, 1, 2)
                    END
                ELSE {clean_col}
            END
        """
    }
    
    conversion_sql = conversions.get(format_detected, clean_col)
    
    # Faixa válida de ano: 1900 até ano_atual (+1 para SINASC)
    current_year = datetime.now().year
    year_margin = 1 if is_sinasc else 0
    max_year = current_year + year_margin
    
    # Após conversão para DDMMYYYY (8 dígitos), corrige ano inválido.
    # O ano está nas posições 5-8 do resultado DDMMYYYY.
    # Se o ano está fora de 1900..max_year, pega os 2 últimos dígitos:
    #   yy <= ano_atual_2dig -> 20xx
    #   yy > ano_atual_2dig  -> 19xx
    fix_year_sql = f"""
        CASE
            WHEN length({conversion_sql}) = 8
                 AND (CAST(substr({conversion_sql}, 5, 4) AS INTEGER) < 1900
                      OR CAST(substr({conversion_sql}, 5, 4) AS INTEGER) > {max_year})
            THEN
                substr({conversion_sql}, 1, 4) ||
                CASE
                    WHEN CAST(substr({conversion_sql}, 7, 2) AS INTEGER) <= {current_year_2dig}
                        THEN '20' || substr({conversion_sql}, 7, 2)
                    ELSE '19' || substr({conversion_sql}, 7, 2)
                END
            ELSE {conversion_sql}
        END
    """
    
    return fix_year_sql

# ========================================================================================
# NAME CLEANING FUNCTIONS
# ========================================================================================

# SQL functions for cleaning and validating name fields (individual name,
# mother's name, father's name). Applies data quality rules defined in UserConfig to
# identify and mark invalid entries.
#
# NAME STANDARDIZATION PROCESS:
# 1. TRIM whitespace from beginning and end
# 2. NORMALIZE text (remove accents using NFD decomposition)
# 3. CONVERT to lowercase
# 4. REMOVE all non-alphabetic characters (keep only letters and spaces)
# 5. COLLAPSE multiple consecutive spaces into single space
# 6. VALIDATE against configured rules (minimum length, invalid terms, regex patterns)
# 7. SET TO NULL if validation fails, otherwise keep cleaned name
#
# These functions generate SQL CASE statements that are executed within DuckDB for
# performance (avoiding row-by-row Python processing). Invalid names are set to NULL,
# which excludes them from comparison during deduplication.

def apply_nome_cleaning(column_value: str, user_config: UserConfig) -> str:
    """Apply name cleaning rules in SQL"""
    if not user_config.aplicar_limpeza_nomes:
        return column_value
    
    termos_exatos_escaped = [t.replace("'", "''") for t in user_config.termos_invalidos_exatos]
    termos_exatos_sql = "', '".join(termos_exatos_escaped)
    
    termos_regex_pattern = '|'.join([f'\\b{termo}\\b' for termo in user_config.termos_invalidos_regex])
    
    cleaning_sql = f"""
        CASE 
            WHEN {column_value} IN ('{termos_exatos_sql}') THEN NULL
            WHEN regexp_matches({column_value}, '{termos_regex_pattern}') THEN NULL
            WHEN length({column_value}) <= {user_config.tamanho_minimo_nome} THEN NULL
            WHEN regexp_matches({column_value}, '^([a-z])\\\\1+$') THEN NULL
            ELSE {column_value}
        END
    """
    
    return cleaning_sql

# ========================================================================================
# STANDARD SEX FOR BLOCKING
# ========================================================================================

# Standardizes sex/gender values using FREQUENCY-BASED INFERENCE from first names.
# This sophisticated approach handles missing/invalid sex data by analyzing name patterns
# across the entire dataset.
#
# SEX STANDARDIZATION PROCESS (2 stages):
#
# STAGE 1 - BASIC SEX STANDARDIZATION (done earlier in pipeline):
#    Converts various representations to standard codes:
#    - 'M': Masculino, MASC, M, Male, 1
#    - 'F': Feminino, FEM, F, Female, 2  
#    - 'I': Ignorado, Ignorada, I (invalid/unknown)
#    - NULL: missing or unrecognized values
#    Result stored in: sexo_std column
#
# STAGE 2 - FREQUENCY-BASED SEX INFERENCE (this function):
#    For each record:
#    1. EXTRACT first name from full name 
#    2. ANALYZE first name frequency across ALL dataset
#       - Groups all records by primeiro_nome_std
#       - Counts how many times each first name appears as 'M' vs 'F' 
#    3. ASSIGN most frequent sex to that first name
#       - If COUNT(M) >= COUNT(F): assign 'M' to all records with that first name
#       - If COUNT(F) > COUNT(M): assign 'F' to all records with that first name
#    4. FALLBACK to original sex if:
#       - First name appears only once in dataset (no frequency pattern)
#       - First name is NULL
#       Result stored in: sexo_blocking column
#
# WHY THIS APPROACH?
# - Handles missing sex data intelligently (infers from name patterns)
# - Corrects obvious data entry errors (e.g., "MARIA" marked as M)
# - Works well for Brazilian names with strong gender associations
# - Leverages the collective information in the dataset
# - Maintains consistency: all people with same first name get same sex for blocking
#
# PERFORMED IN SQL within DuckDB for performance (not row-by-row Python)

def apply_standard_sex(con, tabela_origem='df_concat'):
    """
    Cria sexo_blocking para padronizar sexo baseado no nome mais frequente
    """
    print("\n=== CRIANDO SEXO PADRONIZADO PARA BLOCKING ===")
    
    # Criar a coluna
    con.execute(f"""
        ALTER TABLE {tabela_origem} ADD COLUMN IF NOT EXISTS sexo_blocking VARCHAR
    """)
    
    # Atualizar com sexo padronizado (PARALELO: GROUP BY + JOIN, O(N) — substitui a subconsulta correlacionada O(N²))
    # 1. Maioria de sexo por primeiro nome (so nomes com >1 ocorrencia e sexo valido)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _sexo_freq AS
        SELECT primeiro_nome_std,
               CASE WHEN SUM(CASE WHEN sexo_std = 'M' THEN 1 ELSE 0 END) >=
                         SUM(CASE WHEN sexo_std = 'F' THEN 1 ELSE 0 END) THEN 'M' ELSE 'F' END AS _sexo_maj
        FROM {tabela_origem}
        WHERE sexo_std IS NOT NULL AND sexo_std != ''
        GROUP BY primeiro_nome_std
        HAVING COUNT(*) > 1
    """)
    # 2. Fallback: todos recebem o proprio sexo_std
    con.execute(f"UPDATE {tabela_origem} SET sexo_blocking = sexo_std")
    # 3. Sobrescreve com a maioria onde o nome tem maioria definida (JOIN, nao subconsulta por linha)
    con.execute(f"""
        UPDATE {tabela_origem} AS t
        SET sexo_blocking = sf._sexo_maj
        FROM _sexo_freq sf
        WHERE sf.primeiro_nome_std = t.primeiro_nome_std
    """)
    con.execute("DROP TABLE IF EXISTS _sexo_freq")

# ========================================================================================
# PART 1: FILE READING AND ID CREATION
# ========================================================================================

# First major pipeline stage: Discovers, profiles, and ingests all source files into DuckDB.
# Creates unique global identifiers and preserves complete original data for final output.
#
# MAIN RESPONSIBILITIES:
# 1. FILE DISCOVERY: Scan data folder for supported formats (CSV, DBF, DBC, Parquet, Excel, etc.)
# 2. FORMAT DETECTION: Identify file type from extension
# 3. ENCODING DETECTION: Analyze text files to determine character encoding (latin1, utf-8, cp1252)
# 4. DELIMITER DETECTION: For CSV files, detect separator (comma, semicolon, tab)
# 5. FILE PROFILING: Create FileProfile with format, encoding, delimiter, quote char
# 6. DATA INGESTION: Read files into DuckDB tables using format-specific logic
# 7. COLUMN DETECTION: Map source columns to standardized variable names (nome, data_nascimento, sexo, etc.)
# 8. UNIQUE ID GENERATION: Assign sequential unique_id to every record across all files
# 9. SOURCE TRACKING: Add 'fonte' column with original filename for provenance
# 10. COMPLETE FILE EXPORT: Save files with unique_id to COMPLETE_WITH_ID folder for later joining

def part1_read_files(user_config: UserConfig, con: duckdb.DuckDBPyConnection) -> Dict:
    """PART 1: Read all files and create unique IDs"""
    
    data_path = Path(user_config.data_folder)
    supported_extensions = ['.csv', '.txt', '.parquet', '.dbf', '.xls', '.xlsx']
    
    files = []
    for ext in supported_extensions:
        files.extend(data_path.glob(f"*{ext}"))
    
    if not files:
        raise ValueError(f"No supported files found in {data_path}")
    
    print(f"Found {len(files)} files")
    
    # Detectar SINASC nos arquivos
    file_names_list = [f.stem for f in files]
    sinasc_detectado, sinasc_file = detect_sinasc_in_files(file_names_list)
    validate_sinasc_config(user_config, sinasc_detectado, sinasc_file)

    # Garantir a tabela de metadata e semear file_metadata com bases ja processadas.
    # Permite retomada: bases cujo parquet ja existe nao serao reprocessadas, e seu
    # metadata (date_format, tem_idade, flags SINASC) e recuperado daqui sem abrir a base.
    con.execute("CREATE TABLE IF NOT EXISTS processing_metadata (metadata VARCHAR)")

    file_metadata = {}
    _metadata_existente = con.execute("SELECT metadata FROM processing_metadata").fetchone()
    if _metadata_existente and _metadata_existente[0]:
        _metadata_salvo = json.loads(_metadata_existente[0])
        for _nome_base, _meta in _metadata_salvo.items():
            file_metadata[_nome_base] = {
                'table_name': _meta['table_name'],
                'record_count': _meta['record_count'],
                'mapping': None,
                'date_format': DateFormat(_meta['date_format']) if _meta.get('date_format') else None,
                'output_path': _meta['output_path'],
                'tem_idade_sem_data_nasc': _meta.get('tem_idade_sem_data_nasc', False),
                'is_sinasc_mae': _meta.get('is_sinasc_mae', False),
                'is_sinasc_filho': _meta.get('is_sinasc_filho', False),
            }
        print(f"  Metadata existente carregado: {len(file_metadata)} base(s) ja processada(s)")

    failed_files = []
    
    for file_path in files:
        file_name = file_path.stem

        _parquet_ja_salvo = Path(user_config.complete_files_folder) / f"{file_name}.parquet"
        if _parquet_ja_salvo.exists() and file_name in file_metadata:
            print(f"\n{'='*60}")
            print(f"Skipping (ja processada): {file_path.name}")
            print(f"{'='*60}")
            continue

        print(f"\n{'='*60}")
        print(f"Processing: {file_path.name}")
        print(f"{'='*60}")
        
        try:
            profile = profile_file(file_path, user_config)
            print(f"  Format: {profile.format.value}")
            if profile.encoding:
                print(f"  Encoding: {profile.encoding}")
            if profile.delimiter:
                print(f"  Delimiter: '{profile.delimiter}'")
            
            temp_table = f"temp_{file_name}"
            print(f"  Loading into table: {temp_table}")
            record_count = read_file_to_duckdb(profile, con, temp_table)
            
            columns = con.execute(f"DESCRIBE {temp_table}").fetchall()
            column_names = [col[0] for col in columns]
            print(f"  Columns found ({len(column_names)}): {', '.join(column_names[:5])}{'...' if len(column_names) > 5 else ''}")
            
            print(f"  Detecting variables...")
            is_sinasc_file = sinasc_detectado and 'SINASC' in file_name.upper()

            if is_sinasc_file:
                mapping = detect_sinasc_variables(
                    con, temp_table, user_config, file_name,
                    is_mae=user_config.sinasc_mae
                )
            else:
                mapping = detect_variables(con, temp_table, user_config, file_name)

            # Guardar flag SINASC no metadata
            is_sinasc_mae = is_sinasc_file and user_config.sinasc_mae
            is_sinasc_filho = is_sinasc_file and user_config.sinasc_filho

            if not mapping.tem_idade_sem_data_nasc:
                print(f"  Detecting date format...")
                date_format = detect_date_format(con, temp_table, mapping.data_nascimento)
                print(f"  Date format detected: {date_format.value}")
            else:
                date_format = None
                print(f"  ℹ Skipping date format detection (base uses idade + data_notificacao)")

            processed_table = f"processed_{file_name}"
            print(f"  Creating processed table: {processed_table}")
            
            con.execute(f"""
                CREATE OR REPLACE TABLE {processed_table} AS
                SELECT 
                    '{file_name}_' || ROW_NUMBER() OVER () AS unique_id,
                    *
                FROM {temp_table}
            """)
            
            complete_output_path = Path(user_config.complete_files_folder) / f"{file_name}.parquet"
            con.execute(f"""
                COPY {processed_table} TO '{complete_output_path}' (FORMAT PARQUET)
            """)
            
            print(f"  ✓ SUCCESS: Saved {complete_output_path.name}")

            file_metadata[file_name] = {
                'table_name': processed_table,
                'record_count': record_count,
                'mapping': mapping,
                'date_format': date_format,
                'output_path': str(complete_output_path),
                'tem_idade_sem_data_nasc': mapping.tem_idade_sem_data_nasc,
                'is_sinasc_mae': is_sinasc_mae,
                'is_sinasc_filho': is_sinasc_filho
            }

            _metadata_json_parcial = json.dumps({
                k: {
                    'table_name': v['table_name'],
                    'record_count': v['record_count'],
                    'date_format': v['date_format'].value if v['date_format'] else None,
                    'output_path': v['output_path'],
                    'tem_idade_sem_data_nasc': v.get('tem_idade_sem_data_nasc', False),
                    'is_sinasc_mae': v.get('is_sinasc_mae', False),
                    'is_sinasc_filho': v.get('is_sinasc_filho', False)
                } for k, v in file_metadata.items()
            })
            con.execute("DELETE FROM processing_metadata")
            con.execute(f"INSERT INTO processing_metadata VALUES ('{_metadata_json_parcial}')")

            con.execute(f"DROP TABLE IF EXISTS {temp_table}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ FAILED: {error_msg}")
            failed_files.append({
                'file': file_path.name,
                'error': error_msg
            })
            
            try:
                con.execute(f"DROP TABLE IF EXISTS temp_{file_name}")
            except:
                pass
            
            continue
    
    print(f"\n{'='*60}")
    print(f"PART 1 SUMMARY")
    print(f"{'='*60}")
    print(f"Files found: {len(files)}")
    print(f"Files processed successfully: {len(file_metadata)}")
    print(f"Files failed: {len(failed_files)}")
    
    if file_metadata:
        total_records = sum(v['record_count'] for v in file_metadata.values())
        print(f"\nSuccessfully processed files:")
        for fname, meta in file_metadata.items():
            print(f"  ✓ {fname}: {meta['record_count']:,} records")
        print(f"\nTotal records loaded: {total_records:,}")
    
    if failed_files:
        print(f"\nFailed files:")
        for fail in failed_files:
            print(f"  ✗ {fail['file']}")
            print(f"    Error: {fail['error']}")
    
    if not file_metadata:
        raise RuntimeError("No files were successfully processed. Check the errors above.")
    
    metadata_json = json.dumps({
        k: {
            'table_name': v['table_name'],
            'record_count': v['record_count'],
            'date_format': v['date_format'].value if v['date_format'] else None,
            'output_path': v['output_path'],
            'tem_idade_sem_data_nasc': v.get('tem_idade_sem_data_nasc', False),
            'is_sinasc_mae': v.get('is_sinasc_mae', False),
            'is_sinasc_filho': v.get('is_sinasc_filho', False)
        } for k, v in file_metadata.items()
    })

    con.execute("CREATE TABLE IF NOT EXISTS processing_metadata (metadata VARCHAR)")
    con.execute("DELETE FROM processing_metadata")
    con.execute(f"INSERT INTO processing_metadata VALUES ('{metadata_json}')")
    
    print(f"\n✓ Part 1 complete!")
    
    return file_metadata

# ========================================================================================
# PART 2: STANDARDIZATION
# ========================================================================================

# Second major pipeline stage: Cleans, validates, and standardizes all data for deduplication.
# Transforms raw heterogeneous data into consistent, comparable format required by Splink.
#
# MAIN RESPONSIBILITIES:
# 1. CREATE STANDARDIZED TABLE: Build master_dedup table with only mapped/standardized columns
# 2. EXTRACT FIRST NAMES: Split full names to get primeiro_nome for frequency analysis
# 3. CLEAN NAMES: Apply validation rules to identify invalid entries (IGNORADO, XX, AAAA, etc.)
# 4. STANDARDIZE SEX: Use frequency-based inference to assign sex based on first name patterns
# 5. DETECT DATE FORMATS: Analyze date columns to identify format (DD/MM/YYYY, YYYY-MM-DD, etc.)
# 6. CONVERT DATES: Transform all dates to standardized DDMMYYYY format
# 7. VALIDATE DATA: Mark invalid names as NULL, handle missing values
# 8. CREATE BLOCKING COLUMNS: Prepare ano_nascimento_std, sexo_blocking for blocking strategy
# 9. MEMORY CLEANUP: Free memory by dropping intermediate tables

# KEY FEATURES:
# - All processing done in SQL (DuckDB) for performance - NOT row-by-row Python
# - Preserves original data in COMPLETE_WITH_ID files for final output
# - Invalid data marked as NULL rather than discarded (maintains record count)
# - Frequency-based sex inference leverages collective dataset patterns
# - Date format detection handles multiple Brazilian conventions automatically
# - Memory-efficient: drops intermediate tables after use

def part2_standardize_data(user_config: UserConfig, con: duckdb.DuckDBPyConnection) -> None:
    """PART 2: Standardize data from processed files"""
    
    con.create_function("normalize_text", normalize_text)
    
    metadata_result = con.execute("SELECT metadata FROM processing_metadata").fetchone()
    if not metadata_result:
        raise RuntimeError("No metadata found. Run Part 1 first.")
    
    metadata_dict = json.loads(metadata_result[0])
    
    print("Creating master deduplication table")
    # Registro de bases ja consolidadas no master_dedup (retomada da Part 2).
    # NAO dropamos o master_dedup: bases ja inseridas sao puladas; so as que faltam entram.
    con.execute("CREATE TABLE IF NOT EXISTS master_dedup_bases (file_name VARCHAR)")
    _bases_ja_consolidadas = {
        row[0] for row in con.execute("SELECT file_name FROM master_dedup_bases").fetchall()
    }
    if _bases_ja_consolidadas:
        print(f"  Retomada: {len(_bases_ja_consolidadas)} base(s) ja consolidada(s) no master_dedup")
    
    for file_name, meta in metadata_dict.items():
        if file_name in _bases_ja_consolidadas:
            print(f"Skipping (ja consolidada): {file_name}")
            continue
        print(f"Standardizing: {file_name}")

        table_name = meta['table_name']

        tem_idade = meta.get('tem_idade_sem_data_nasc', False)
        is_sinasc_mae = meta.get('is_sinasc_mae', False)
        is_sinasc_filho = meta.get('is_sinasc_filho', False)

        if is_sinasc_mae:
            print(f"  ℹ SINASC MAE mode: NOMEMAE→nome_std, DTNASCMAE→data_nasc, sexo=F forced")
        elif is_sinasc_filho:
            print(f"  ℹ SINASC FILHO mode: NOMEMAE→nome_std, DTNASC→data_nasc, NOMEPAI→support")

        is_sinasc_file = is_sinasc_mae or is_sinasc_filho
        if is_sinasc_file:
            temp_mapping = detect_sinasc_variables(
                con, table_name, user_config, file_name,
                is_mae=is_sinasc_mae
            )
        else:
            temp_mapping = detect_variables(con, table_name, user_config, file_name)

        if not tem_idade:
            date_format = DateFormat(meta['date_format'])
            date_sql = get_date_conversion_sql(date_format, temp_mapping.data_nascimento, is_sinasc=is_sinasc_file)
        else:
            date_format = None
            date_sql = None
            # Detectar tipo da data de notificação
            tipo_data_notific = detect_data_notificacao_type(con, table_name, temp_mapping.data_notificacao)
            
            if tipo_data_notific == 'data_completa':
                # Tratar como data, detectar formato, extrair ano
                notific_date_format = detect_date_format(con, table_name, temp_mapping.data_notificacao)
                notific_date_sql = get_date_conversion_sql(notific_date_format, temp_mapping.data_notificacao, is_sinasc=is_sinasc_file)
                # Extrair ano (posição 5-8 da string DDMMYYYY)
                ano_notific_sql = f"CAST(substr(({notific_date_sql}), 5, 4) AS INTEGER)"
            else:
                # 2 ou 4 dígitos — usar função dedicada
                ano_notific_sql = get_ano_notificacao_sql(
                    tipo_data_notific, temp_mapping.data_notificacao, temp_mapping.idade
                )

        nome_normalized = f"normalize_text(CAST({temp_mapping.nome} AS VARCHAR))"
        nome_mae_normalized = f"normalize_text(CAST({temp_mapping.nome_mae} AS VARCHAR))" if temp_mapping.nome_mae else 'CAST(NULL AS VARCHAR)'
        nome_pai_normalized = f"normalize_text(CAST({temp_mapping.nome_pai} AS VARCHAR))" if temp_mapping.nome_pai else 'CAST(NULL AS VARCHAR)'
        
        nome_cleaned = apply_nome_cleaning(nome_normalized, user_config)
        nome_mae_cleaned = apply_nome_cleaning(nome_mae_normalized, user_config) if temp_mapping.nome_mae else 'CAST(NULL AS VARCHAR)'
        nome_pai_cleaned = apply_nome_cleaning(nome_pai_normalized, user_config) if temp_mapping.nome_pai else 'CAST(NULL AS VARCHAR)'
        
        if not tem_idade:
            # ===== FLUXO NORMAL: base com data de nascimento =====
            standardization_sql = f"""
                SELECT 
                    unique_id,
                    CAST(NULL AS VARCHAR) AS unique_id_original,
                    '{get_normalized_source(file_name)}' AS fonte,
                    ({nome_cleaned}) AS nome_std,
                    ({nome_mae_cleaned}) AS nome_mae_std,
                    ({nome_pai_cleaned}) AS nome_pai_std,
                    ({date_sql}) AS data_nascimento_std,
                    CASE 
                        WHEN length(substr(({date_sql}), 5, 4)) = 4 
                        THEN CAST(substr(({date_sql}), 5, 4) AS INTEGER)
                        ELSE NULL
                    END AS ano_nascimento,
                    {f"regexp_replace(CAST({temp_mapping.codigo_municipio} AS VARCHAR), '[^0-9]', '', 'g')" if temp_mapping.codigo_municipio else "CAST(NULL AS VARCHAR)"} AS codigo_municipio_std,
                    {f"CAST('F' AS VARCHAR)" if is_sinasc_mae else f'''CASE 
                        WHEN lower(CAST({temp_mapping.sexo} AS VARCHAR)) IN ('m', 'masculino', 'homem', '0') THEN 'M'
                        WHEN lower(CAST({temp_mapping.sexo} AS VARCHAR)) IN ('f', 'feminino', 'mulher', '1') THEN 'F'
                        ELSE ''
                    END''' if temp_mapping.sexo else "CAST('' AS VARCHAR)"} AS sexo_std,
                    0 AS origem_idade,
                    NULL AS ano_exato_idade
                FROM {table_name}
            """
        else:
            # ===== FLUXO IDADE: base sem data de nascimento =====
            idade_col = f"CAST({temp_mapping.idade} AS INTEGER)"            
            standardization_sql = f"""
                SELECT 
                    CASE 
                        WHEN ano_offset.val = -1 THEN unique_id || '_tm1'
                        WHEN ano_offset.val = 0 THEN unique_id || '_t0'
                        WHEN ano_offset.val = 1 THEN unique_id || '_tp1'
                    END AS unique_id,
                    unique_id AS unique_id_original,
                    '{get_normalized_source(file_name)}' AS fonte,
                    ({nome_cleaned}) AS nome_std,
                    ({nome_mae_cleaned}) AS nome_mae_std,
                    ({nome_pai_cleaned}) AS nome_pai_std,
                    CAST(NULL AS VARCHAR) AS data_nascimento_std,
                    ({ano_notific_sql}) - {idade_col} + ano_offset.val AS ano_nascimento,
                    {f"regexp_replace(CAST({temp_mapping.codigo_municipio} AS VARCHAR), '[^0-9]', '', 'g')" if temp_mapping.codigo_municipio else "CAST(NULL AS VARCHAR)"} AS codigo_municipio_std,
                    {f'''CASE 
                        WHEN lower(CAST({temp_mapping.sexo} AS VARCHAR)) IN ('m', 'masculino', 'homem', '0') THEN 'M'
                        WHEN lower(CAST({temp_mapping.sexo} AS VARCHAR)) IN ('f', 'feminino', 'mulher', '1') THEN 'F'
                        ELSE ''
                    END''' if temp_mapping.sexo else "CAST('' AS VARCHAR)"} AS sexo_std,
                    1 AS origem_idade,
                    ({ano_notific_sql}) - {idade_col} AS ano_exato_idade
                FROM {table_name}
                CROSS JOIN (SELECT -1 AS val UNION ALL SELECT 0 UNION ALL SELECT 1) AS ano_offset
                WHERE {idade_col} IS NOT NULL 
                AND CAST({temp_mapping.data_notificacao} AS VARCHAR) IS NOT NULL
                AND CAST({temp_mapping.data_notificacao} AS VARCHAR) != ''
            """
            
            # Log de registros descartados por falta de informação
            total_base = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            total_com_info = con.execute(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE CAST({temp_mapping.idade} AS INTEGER) IS NOT NULL
                AND CAST({temp_mapping.data_notificacao} AS VARCHAR) IS NOT NULL
                AND CAST({temp_mapping.data_notificacao} AS VARCHAR) != ''
            """).fetchone()[0]
            descartados = total_base - total_com_info
            if descartados > 0:
                print(f"  ⚠ {file_name}: {descartados:,} records discarded (missing idade or data_notificacao)")
                print(f"    Records with valid info: {total_com_info:,} (will be triplicated to {total_com_info * 3:,})")
        
        con.execute("BEGIN TRANSACTION")
        try:
            if con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'master_dedup'").fetchone()[0] == 0:
                con.execute(f"CREATE OR REPLACE TABLE master_dedup AS {standardization_sql}")
            else:
                con.execute(f"INSERT INTO master_dedup {standardization_sql}")
            con.execute("INSERT INTO master_dedup_bases VALUES (?)", [file_name])
            con.execute("COMMIT")
            _bases_ja_consolidadas.add(file_name)
        except Exception:
            con.execute("ROLLBACK")
            raise
    
    # Garantir que coluna unique_id_original existe
    try:
        con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS unique_id_original VARCHAR")
    except:
        pass

    # Garantir que colunas origem_idade e ano_exato_idade existem
    try:
        con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS origem_idade INTEGER DEFAULT 0")
    except:
        pass
    try:
        con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS ano_exato_idade INTEGER")
    except:
        pass

    #Criar colunas de primeiro nome
    print("Adding first name columns...")
    con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS primeiro_nome_std VARCHAR")
    con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS primeiro_nome_mae_std VARCHAR")
    con.execute("ALTER TABLE master_dedup ADD COLUMN IF NOT EXISTS primeiro_nome_pai_std VARCHAR")
    
    con.execute("""
        UPDATE master_dedup SET
            primeiro_nome_std = CASE 
                WHEN nome_std IS NOT NULL THEN split_part(nome_std, ' ', 1)
                ELSE NULL
            END,
            primeiro_nome_mae_std = CASE 
                WHEN nome_mae_std IS NOT NULL THEN split_part(nome_mae_std, ' ', 1)
                ELSE NULL
            END,
            primeiro_nome_pai_std = CASE 
                WHEN nome_pai_std IS NOT NULL THEN split_part(nome_pai_std, ' ', 1)
                ELSE NULL
            END
    """)    

    apply_standard_sex(con, 'master_dedup')

    # === FORÇAR TIPO VARCHAR PARA COLUNAS DE TEXTO (evita inferência errada do DuckDB) ===
    print("  Forcing VARCHAR type for text columns...")
    colunas_texto = ['nome_std', 'nome_mae_std', 'nome_pai_std', 
                    'primeiro_nome_std', 'primeiro_nome_mae_std', 'primeiro_nome_pai_std',
                    'sexo_std', 'sexo_blocking', 'codigo_municipio_std', 'fonte', 'unique_id']

    for col in colunas_texto:
        try:
            con.execute(f"ALTER TABLE master_dedup ALTER COLUMN {col} SET DATA TYPE VARCHAR")
        except:
            pass  # Coluna pode não existir ou já ser VARCHAR

    # Detectar se alguma base tem idade sem data de nascimento
    tem_base_sem_data_nascimento = con.execute("""
        SELECT COUNT(*) FROM master_dedup WHERE origem_idade = 1
    """).fetchone()[0] > 0

    if tem_base_sem_data_nascimento:
        total_triplicados = con.execute("SELECT COUNT(*) FROM master_dedup WHERE origem_idade = 1").fetchone()[0]
        total_ids_unicos_triplicados = con.execute("SELECT COUNT(DISTINCT unique_id) FROM master_dedup WHERE origem_idade = 1").fetchone()[0]
        print(f"\n  ℹ IDADE MODE DETECTED:")
        print(f"    Records with origem_idade=1: {total_triplicados:,} (from {total_ids_unicos_triplicados:,} unique individuals)")
        print(f"    Splink will use nome_std only (no data_nascimento comparison)")

    total_records = con.execute("SELECT COUNT(*) FROM master_dedup").fetchone()[0]
    standardized_path = Path(user_config.results_folder) / "base_padronizada_pre_dedup.parquet"
    con.execute(f"COPY master_dedup TO '{standardized_path}' (FORMAT PARQUET)")
    
    print(f"Part 2 complete: {total_records:,} standardized records")
    print(f"Saved: {standardized_path}")
    
    # Memory cleanup before deduplication
    tables_to_drop = con.execute("SHOW TABLES").fetchall()
    for table in tables_to_drop:
        table_name = table[0]
        if table_name.startswith('processed_') or table_name.startswith('temp_'):
            print(f"  Dropping table: {table_name}")
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    gc.collect()
    
    available_gb = get_available_memory_gb()
    print(f"  Available memory after cleanup: {available_gb:.2f} GB")
    print("="*60 + "\n")

# ========================================================================================
# PART 3: DEDUPLICATION (SPLINK)
# ========================================================================================
# Performs probabilistic record linkage using Splink to identify duplicate records across the dataset.
# Uses Expectation-Maximization algorithm to calculate match probabilities, followed by optional rule-based 
# refinement and clustering.
#
# MAIN RESPONSIBILITIES:
# 1. PREPARE SPLINK INPUT: Select standardized columns from master_dedup for linkage
# 2. CONFIGURE BLOCKING: Define blocking rules to reduce comparison space (year, sex, municipality)
# 3. CONFIGURE COMPARISONS: Set up fuzzy matching with Levenshtein thresholds for each variable
# 4. TRAIN MODEL: Use Expectation-Maximization to learn match/non-match probabilities
# 5. PREDICT PAIRS: Generate candidate duplicate pairs with match probabilities
# 6. REFINE DECISIONS: Apply rule-based logic to borderline pairs (optional)
# 7. CLUSTER RECORDS: Group duplicates transitively using Union-Find algorithm
# 8. ASSIGN IDS: Create cluster_id and id_global for linked records
# 9. GENERATE OUTPUTS: Create multiple result files with match information
# 10. CREATE FINAL DATASETS: Join deduplication results with original complete data

# MODIFIED: Processes data block by block (by birth year) to manage memory usage.
# Each year block is processed independently and results are appended to DuckDB tables.
# Final consolidation generates global IDs and exports to Parquet.

def init_append_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Initialize DuckDB tables for appending results across year blocks"""
    
    # Tabela para acumular todos os pares auditados
    con.execute("""
        CREATE TABLE IF NOT EXISTS pares_auditoria_acumulado (
            unique_id_l VARCHAR,
            unique_id_r VARCHAR,
            fonte_l VARCHAR,
            fonte_r VARCHAR,
            nome_std_l VARCHAR,
            nome_std_r VARCHAR,
            nome_mae_std_l VARCHAR,
            nome_mae_std_r VARCHAR,
            nome_pai_std_l VARCHAR,
            nome_pai_std_r VARCHAR,
            primeiro_nome_std_l VARCHAR,
            primeiro_nome_std_r VARCHAR,
            primeiro_nome_mae_std_l VARCHAR,
            primeiro_nome_mae_std_r VARCHAR,
            primeiro_nome_pai_std_l VARCHAR,
            primeiro_nome_pai_std_r VARCHAR,
            data_nascimento_std_l VARCHAR,
            data_nascimento_std_r VARCHAR,
            ano_nascimento_l INTEGER,
            ano_nascimento_r INTEGER,
            sexo_std_l VARCHAR,
            sexo_std_r VARCHAR,
            nome_score_100 FLOAT,
            mae_score_100 FLOAT,
            pai_score_100 FLOAT,
            data_score_100 FLOAT,
            primeiro_nome_score_100 FLOAT,
            primeiro_nome_mae_score_100 FLOAT,
            primeiro_nome_pai_score_100 FLOAT,
            primeiro_nome_grudado_score_100 FLOAT,
            primeiro_nome_mae_grudado_score_100 FLOAT,
            primeiro_nome_pai_grudado_score_100 FLOAT,
            decisao_refinada INTEGER,
            decisao_final INTEGER,
            decision_reason VARCHAR,
            tamanho_primeiro_nome INTEGER,
            tamanho_primeiro_nome_mae INTEGER,
            tamanho_primeiro_nome_pai INTEGER,
            origem_idade_l INTEGER,
            origem_idade_r INTEGER,
            ano_exato_idade_l INTEGER,
            ano_exato_idade_r INTEGER,
            unique_id_original_l VARCHAR,
            unique_id_original_r VARCHAR,
            ano_bloco INTEGER,
            sobrenome_faltantes_pessoa INTEGER,
            sobrenome_trocados_pessoa INTEGER,
            sobrenome_faltantes_mae INTEGER,
            sobrenome_trocados_mae INTEGER,
            sobrenome_faltantes_pai INTEGER,
            sobrenome_trocados_pai INTEGER,
            match_probability FLOAT
        )
    """)
    
    print("  ✓ Append tables initialized")

def estimate_memory_per_year(con: duckdb.DuckDBPyConnection, standardized_path: Path) -> Dict[int, Dict]:
    """
    Estimate memory usage for each year based on record count per sex block.
    Considers that blocking is done by ano_nascimento AND sexo_blocking.
    Returns dict: {ano: {'records': n, 'pairs_estimated': p, 'memory_mb': m}}
    """
    print("\nEstimating memory requirements per year (considering sex blocking)...")
    
    # Contar registros por ano E sexo para calcular pares reais do blocking
    year_sex_counts = con.execute(f"""
        SELECT 
            ano_nascimento,
            SUM(n) as total_records,
            SUM((n * (n - 1)) / 2) as pairs_estimated
        FROM (
            SELECT 
                ano_nascimento, 
                sexo_blocking, 
                COUNT(*) as n
            FROM read_parquet('{standardized_path}')
            WHERE ano_nascimento IS NOT NULL
            GROUP BY ano_nascimento, sexo_blocking
        )
        GROUP BY ano_nascimento
        ORDER BY ano_nascimento
    """).fetchall()
    
    # Bytes por par - ajuste baseado em observações empíricas
    BYTES_PER_PAIR = 0.2
    
    estimates = {}
    for ano, total_records, pairs_estimated in year_sex_counts:
        memory_bytes = pairs_estimated * BYTES_PER_PAIR
        memory_mb = memory_bytes / (1024 * 1024)
        
        estimates[ano] = {
            'records': total_records,
            'pairs_estimated': pairs_estimated,
            'memory_mb': memory_mb
        }
    
    return estimates

def group_years_by_memory(estimates: Dict[int, Dict], available_memory_mb: float, 
                          memory_percentage: float = 0.75) -> List[List[int]]:
    """
    Group years into batches that fit within available memory.
    Returns list of year groups: [[1990, 1991, 1992], [1993, 1994], ...]
    """
    memory_limit_mb = available_memory_mb * memory_percentage
    
    print(f"\nGrouping years by memory limit: {memory_limit_mb:.0f} MB ({memory_percentage*100:.0f}% of {available_memory_mb:.0f} MB)")
    
    years_sorted = sorted(estimates.keys())
    groups = []
    current_group = []
    current_memory = 0.0
    
    for ano in years_sorted:
        year_memory = estimates[ano]['memory_mb']
        
        # Se um único ano excede o limite, ele vai sozinho
        if year_memory > memory_limit_mb:
            if current_group:
                groups.append(current_group)
            groups.append([ano])
            current_group = []
            current_memory = 0.0
            print(f"  ⚠ Year {ano} exceeds limit alone ({year_memory:.0f} MB) - processing solo")
            continue
        
        # Se adicionar este ano excede o limite, fecha o grupo atual
        if current_memory + year_memory > memory_limit_mb:
            if current_group:
                groups.append(current_group)
            current_group = [ano]
            current_memory = year_memory
        else:
            current_group.append(ano)
            current_memory += year_memory
    
    # Adicionar último grupo
    if current_group:
        groups.append(current_group)
    
    # Log dos grupos
    print(f"\nYear groups created: {len(groups)}")
    for i, group in enumerate(groups):
        group_memory = sum(estimates[ano]['memory_mb'] for ano in group)
        group_records = sum(estimates[ano]['records'] for ano in group)
        print(f"  Group {i+1}: years {min(group)}-{max(group)} ({len(group)} years, {group_records:,} records, ~{group_memory:.0f} MB)")
    
    return groups

def detectar_campos_com_dados(con: duckdb.DuckDBPyConnection, tabela: str) -> dict:
    """
    Verifica quais campos opcionais realmente têm dados (não são 100% NULL).
    Retorna dict com campo -> bool (True se tem dados).
    """
    campos_opcionais = [
        'nome_mae_std', 'nome_pai_std', 
        'primeiro_nome_mae_std', 'primeiro_nome_pai_std'
    ]
    campos_com_dados = {}
    
    for campo in campos_opcionais:
        try:
            result = con.execute(f"""
                SELECT COUNT(*) as cnt 
                FROM {tabela} 
                WHERE CAST({campo} AS VARCHAR) IS NOT NULL 
                  AND CAST({campo} AS VARCHAR) != ''
                LIMIT 1
            """).fetchone()
            campos_com_dados[campo] = result[0] > 0
        except:
            campos_com_dados[campo] = False
    
    campos_ativos = [k for k, v in campos_com_dados.items() if v]
    campos_vazios = [k for k, v in campos_com_dados.items() if not v]
    
    if campos_ativos:
        print(f"  Campos opcionais COM dados: {campos_ativos}")
    if campos_vazios:
        print(f"  Campos opcionais VAZIOS (serão ignorados no score): {campos_vazios}")
    
    return campos_com_dados

def gerar_sql_scores(campos_com_dados: dict) -> dict:
    """
    Gera os fragmentos SQL para cálculo de scores baseado nos campos disponíveis.
    Retorna dict com 'select_scores', 'select_tamanhos' para usar na query.
    """
    
    # Campos que SEMPRE existem (obrigatórios)
    select_scores = """
            CASE 
                WHEN CAST(ml.nome_std AS VARCHAR) IS NULL OR CAST(mr.nome_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.nome_std AS VARCHAR), CAST(mr.nome_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.nome_std AS VARCHAR)), length(CAST(mr.nome_std AS VARCHAR))) AS FLOAT))
            END AS score_nome,
            
            CASE 
                WHEN CAST(ml.data_nascimento_std AS VARCHAR) IS NULL OR CAST(mr.data_nascimento_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.data_nascimento_std AS VARCHAR), CAST(mr.data_nascimento_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.data_nascimento_std AS VARCHAR)), length(CAST(mr.data_nascimento_std AS VARCHAR))) AS FLOAT))
            END AS score_data_nascimento,
            
            CASE 
                WHEN CAST(ml.codigo_municipio_std AS VARCHAR) IS NULL OR CAST(mr.codigo_municipio_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.codigo_municipio_std AS VARCHAR), CAST(mr.codigo_municipio_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.codigo_municipio_std AS VARCHAR)), length(CAST(mr.codigo_municipio_std AS VARCHAR))) AS FLOAT))
            END AS score_municipio,
            
            CASE 
                WHEN CAST(ml.primeiro_nome_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.primeiro_nome_std AS VARCHAR), CAST(mr.primeiro_nome_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.primeiro_nome_std AS VARCHAR)), length(CAST(mr.primeiro_nome_std AS VARCHAR))) AS FLOAT))           
            END AS score_primeiro_nome,
            
            -- Score de nome grudado (primeiro+segundo do lado menor vs primeiro do maior)
            CASE
                WHEN CAST(ml.primeiro_nome_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_std AS VARCHAR) IS NULL THEN NULL
                WHEN length(CAST(ml.primeiro_nome_std AS VARCHAR)) <= length(CAST(mr.primeiro_nome_std AS VARCHAR)) THEN
                    CASE
                        WHEN split_part(CAST(ml.nome_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(ml.nome_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_std AS VARCHAR), ' ', 2),
                            CAST(mr.primeiro_nome_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(ml.nome_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_std AS VARCHAR), ' ', 2)),
                            length(CAST(mr.primeiro_nome_std AS VARCHAR))
                        ) AS FLOAT))
                    END
                ELSE
                    CASE
                        WHEN split_part(CAST(mr.nome_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(mr.nome_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_std AS VARCHAR), ' ', 2),
                            CAST(ml.primeiro_nome_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(mr.nome_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_std AS VARCHAR), ' ', 2)),
                            length(CAST(ml.primeiro_nome_std AS VARCHAR))
                        ) AS FLOAT))
                    END
            END AS score_primeiro_nome_grudado,
"""
    select_tamanhos = """
            CASE 
                WHEN CAST(ml.primeiro_nome_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_std AS VARCHAR) IS NULL THEN NULL
                ELSE LEAST(length(CAST(ml.primeiro_nome_std AS VARCHAR)), length(CAST(mr.primeiro_nome_std AS VARCHAR)))
            END AS tamanho_primeiro_nome,
"""

    # Campos OPCIONAIS - só adiciona se tiver dados
    if campos_com_dados.get('nome_mae_std', False):
        select_scores += """
            CASE 
                WHEN CAST(ml.nome_mae_std AS VARCHAR) IS NULL OR CAST(mr.nome_mae_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.nome_mae_std AS VARCHAR), CAST(mr.nome_mae_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.nome_mae_std AS VARCHAR)), length(CAST(mr.nome_mae_std AS VARCHAR))) AS FLOAT))
            END AS score_nome_mae,
"""
    else:
        select_scores += """
            NULL AS score_nome_mae,
"""

    if campos_com_dados.get('nome_pai_std', False):
        select_scores += """
            CASE 
                WHEN CAST(ml.nome_pai_std AS VARCHAR) IS NULL OR CAST(mr.nome_pai_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.nome_pai_std AS VARCHAR), CAST(mr.nome_pai_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.nome_pai_std AS VARCHAR)), length(CAST(mr.nome_pai_std AS VARCHAR))) AS FLOAT))
            END AS score_nome_pai,
"""
    else:
        select_scores += """
            NULL AS score_nome_pai,
"""

    if campos_com_dados.get('primeiro_nome_mae_std', False):
        select_scores += """
            CASE 
                WHEN CAST(ml.primeiro_nome_mae_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_mae_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.primeiro_nome_mae_std AS VARCHAR), CAST(mr.primeiro_nome_mae_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.primeiro_nome_mae_std AS VARCHAR)), length(CAST(mr.primeiro_nome_mae_std AS VARCHAR))) AS FLOAT))
            END AS score_primeiro_nome_mae,
            
            CASE
                WHEN CAST(ml.primeiro_nome_mae_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_mae_std AS VARCHAR) IS NULL THEN NULL
                WHEN length(CAST(ml.primeiro_nome_mae_std AS VARCHAR)) <= length(CAST(mr.primeiro_nome_mae_std AS VARCHAR)) THEN
                    CASE
                        WHEN split_part(CAST(ml.nome_mae_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(ml.nome_mae_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_mae_std AS VARCHAR), ' ', 2),
                            CAST(mr.primeiro_nome_mae_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(ml.nome_mae_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_mae_std AS VARCHAR), ' ', 2)),
                            length(CAST(mr.primeiro_nome_mae_std AS VARCHAR))
                        ) AS FLOAT))
                    END
                ELSE
                    CASE
                        WHEN split_part(CAST(mr.nome_mae_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(mr.nome_mae_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_mae_std AS VARCHAR), ' ', 2),
                            CAST(ml.primeiro_nome_mae_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(mr.nome_mae_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_mae_std AS VARCHAR), ' ', 2)),
                            length(CAST(ml.primeiro_nome_mae_std AS VARCHAR))
                        ) AS FLOAT))
                    END
            END AS score_primeiro_nome_mae_grudado,
"""
        select_tamanhos += """
            CASE 
                WHEN CAST(ml.primeiro_nome_mae_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_mae_std AS VARCHAR) IS NULL THEN NULL
                ELSE LEAST(length(CAST(ml.primeiro_nome_mae_std AS VARCHAR)), length(CAST(mr.primeiro_nome_mae_std AS VARCHAR)))
            END AS tamanho_primeiro_nome_mae,
"""
    else:
        select_scores += """
            NULL AS score_primeiro_nome_mae,
            NULL AS score_primeiro_nome_mae_grudado,
"""
        select_tamanhos += """
            NULL AS tamanho_primeiro_nome_mae,
"""

    if campos_com_dados.get('primeiro_nome_pai_std', False):
        select_scores += """
            CASE 
                WHEN CAST(ml.primeiro_nome_pai_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_pai_std AS VARCHAR) IS NULL THEN NULL
                ELSE 1.0 - (CAST(levenshtein(CAST(ml.primeiro_nome_pai_std AS VARCHAR), CAST(mr.primeiro_nome_pai_std AS VARCHAR)) AS FLOAT) / 
                        CAST(GREATEST(length(CAST(ml.primeiro_nome_pai_std AS VARCHAR)), length(CAST(mr.primeiro_nome_pai_std AS VARCHAR))) AS FLOAT))
            END AS score_primeiro_nome_pai,
            
            CASE
                WHEN CAST(ml.primeiro_nome_pai_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_pai_std AS VARCHAR) IS NULL THEN NULL
                WHEN length(CAST(ml.primeiro_nome_pai_std AS VARCHAR)) <= length(CAST(mr.primeiro_nome_pai_std AS VARCHAR)) THEN
                    CASE
                        WHEN split_part(CAST(ml.nome_pai_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(ml.nome_pai_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_pai_std AS VARCHAR), ' ', 2),
                            CAST(mr.primeiro_nome_pai_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(ml.nome_pai_std AS VARCHAR), ' ', 1) || split_part(CAST(ml.nome_pai_std AS VARCHAR), ' ', 2)),
                            length(CAST(mr.primeiro_nome_pai_std AS VARCHAR))
                        ) AS FLOAT))
                    END
                ELSE
                    CASE
                        WHEN split_part(CAST(mr.nome_pai_std AS VARCHAR), ' ', 2) = '' THEN NULL
                        ELSE 1.0 - (CAST(levenshtein(
                            split_part(CAST(mr.nome_pai_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_pai_std AS VARCHAR), ' ', 2),
                            CAST(ml.primeiro_nome_pai_std AS VARCHAR)
                        ) AS FLOAT) / CAST(GREATEST(
                            length(split_part(CAST(mr.nome_pai_std AS VARCHAR), ' ', 1) || split_part(CAST(mr.nome_pai_std AS VARCHAR), ' ', 2)),
                            length(CAST(ml.primeiro_nome_pai_std AS VARCHAR))
                        ) AS FLOAT))
                    END
            END AS score_primeiro_nome_pai_grudado,
"""
        select_tamanhos += """
            CASE 
                WHEN CAST(ml.primeiro_nome_pai_std AS VARCHAR) IS NULL OR CAST(mr.primeiro_nome_pai_std AS VARCHAR) IS NULL THEN NULL
                ELSE LEAST(length(CAST(ml.primeiro_nome_pai_std AS VARCHAR)), length(CAST(mr.primeiro_nome_pai_std AS VARCHAR)))
            END AS tamanho_primeiro_nome_pai,
"""
    else:
        select_scores += """
            NULL AS score_primeiro_nome_pai,
            NULL AS score_primeiro_nome_pai_grudado,
"""
        select_tamanhos += """
            NULL AS tamanho_primeiro_nome_pai,
"""

    return {
        'select_scores': select_scores,
        'select_tamanhos': select_tamanhos
    }

def process_year_block(user_config: UserConfig, con: duckdb.DuckDBPyConnection, 
                       anos: List[int], standardized_path: Path, passada: int = 1) -> int:
    """
    Process deduplication for a group of years (loaded together, but blocking still by year).
    Returns number of approved pairs for this group.
    """
    # Identificador do grupo para logs
    if len(anos) == 1:
        grupo_str = str(anos[0])
    else:
        grupo_str = f"{min(anos)}-{max(anos)} ({len(anos)} years)"
    
    print(f"\n{'='*60}")
    print(f"PROCESSING YEAR GROUP: {grupo_str}")
    print(f"{'='*60}")
    
    monitor_init_block(grupo_str)

    # 1. Carregar dados conforme a passada
    anos_list = ", ".join(str(a) for a in anos)
    
    if passada == 1:
        # Passada 1: apenas registros com data de nascimento real
        print(f"  [PASSADA 1] Loading data for years: {anos_list}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE master_dedup_block AS
            SELECT * FROM read_parquet('{standardized_path}')
            WHERE ano_nascimento IN ({anos_list})
              AND origem_idade = 0
        """)
    else:
        # Passada 2: TODOS os registros desses anos (idade + já deduplicados)
        print(f"  [PASSADA 2] Loading ALL data for years with age records: {anos_list}...")
        con.execute(f"""
            CREATE OR REPLACE TABLE master_dedup_block AS
            SELECT * FROM read_parquet('{standardized_path}')
            WHERE ano_nascimento IN ({anos_list})
        """)
    
    block_count = con.execute("SELECT COUNT(*) FROM master_dedup_block").fetchone()[0]
    print(f"  Records in group: {block_count:,}")
    
    monitor("01_dados_carregados", grupo_str, block_count)

    # Detectar quais campos opcionais têm dados neste bloco
    campos_com_dados = detectar_campos_com_dados(con, "master_dedup_block")
    sql_fragments = gerar_sql_scores(campos_com_dados)

    if block_count == 0:
        print(f"  ⚠ No records for group {grupo_str}, skipping...")
        con.execute("DROP TABLE IF EXISTS master_dedup_block")
        return 0
    
    if block_count == 1:
        print(f"  ⚠ Only 1 record for group {grupo_str}, no pairs possible, skipping...")
        con.execute("DROP TABLE IF EXISTS master_dedup_block")
        return 0
    
    # 2. Configurar Splink para este bloco
    db_api = DuckDBAPI(connection=con)
    
    selected_blocks = []
    if user_config.usar_blocagem_ano:
        selected_blocks.append("l.ano_nascimento = r.ano_nascimento")
    if user_config.usar_blocagem_sexo:
        selected_blocks.append("l.sexo_blocking = r.sexo_blocking")
    if user_config.usar_blocagem_municipio:
        selected_blocks.append("(l.codigo_municipio_std = r.codigo_municipio_std "
                               "OR l.codigo_municipio_std IS NULL "
                               "OR r.codigo_municipio_std IS NULL)")
    
    if not selected_blocks:
        selected_blocks.append("l.ano_nascimento = r.ano_nascimento")
    
    blocking_rule_sql = " AND ".join(selected_blocks)
    # Adiciona condição que impede comparações SIM × SIM e CADUNICO x CADUNICO
    # (base de referência não deduplica entre si)
    sim_filter = " AND (l.fonte != 'SIM' OR r.fonte != 'SIM')" \
                 " AND (l.fonte != 'CADUNICO' OR r.fonte != 'CADUNICO')"

    # Para SINASC: verificar se existe SINASC nos dados E se é modo filho
    # Se filho=True, não deduplica SINASC×SINASC (cada registro é um indivíduo diferente)
    # Se mae=True ou SINASC não existe, não adiciona filtro
    sinasc_filter = ""
    tem_sinasc_nos_dados = con.execute("""
        SELECT COUNT(*) FROM master_dedup_block WHERE fonte = 'SINASC'
    """).fetchone()[0] > 0

    if tem_sinasc_nos_dados and user_config.sinasc_filho:
        sinasc_filter = " AND (l.fonte != 'SINASC' OR r.fonte != 'SINASC')"
        print(f"  ℹ SINASC FILHO mode: blocking SINASC×SINASC pairs")

    # Na Passada 2, impedir pares entre registros que já foram deduplicados na Passada 1
    idade_filter = ""
    if passada == 2:
        idade_filter = " AND (l.origem_idade = 1 OR r.origem_idade = 1)"
        print(f"  ℹ PASSADA 2: blocking requires at least one age-only record per pair")

    # Gate de nome empurrado PARA DENTRO do blocking (filtra no cruzamento, nao depois):
    #   P1 (so datas reais, Caso 1) -> threshold_nome (0.50) | P2 (envolve idade, Caso 2) -> 0.85
    # Espelha o WHERE de Caso 1/Caso 2 que ja roda no pares_auditoria -> mesmo resultado, sem materializar lixo.
    limiar_nome_bloco = user_config.threshold_nome if passada == 1 else 0.85
    name_gate = (
        " AND (CASE "
        "WHEN CAST(l.nome_std AS VARCHAR) IS NULL OR CAST(r.nome_std AS VARCHAR) IS NULL THEN 0 "
        "ELSE 1.0 - (CAST(levenshtein(CAST(l.nome_std AS VARCHAR), CAST(r.nome_std AS VARCHAR)) AS FLOAT) "
        "/ CAST(GREATEST(length(CAST(l.nome_std AS VARCHAR)), length(CAST(r.nome_std AS VARCHAR))) AS FLOAT)) "
        f"END) >= {limiar_nome_bloco}"
    )
    blocking_rule_with_sim_filter = f"{blocking_rule_sql}{sim_filter}{sinasc_filter}{idade_filter}{name_gate}"
    # CustomRule: aceita SQL cru + salting (paraleliza apesar da chave de baixa cardinalidade)
    blocking_rules = [CustomRule(blocking_rule_with_sim_filter, salting_partitions=user_config.salting_partitions)]
    
    # Verificar se este bloco contém registros com origem_idade
    tem_idade_no_bloco = con.execute("""
        SELECT COUNT(*) FROM master_dedup_block WHERE origem_idade = 1
    """).fetchone()[0] > 0

    settings = SettingsCreator(
        link_type="dedupe_only",
        unique_id_column_name="unique_id",
        blocking_rules_to_generate_predictions=blocking_rules
    )
    
    settings.comparisons.append({
        "output_column_name": "nome_std",
        "comparison_levels": [
            {"sql_condition": "nome_std_l IS NULL OR nome_std_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
            {"sql_condition": f"levenshtein(nome_std_l, nome_std_r) / CAST(GREATEST(length(nome_std_l), length(nome_std_r)) AS FLOAT) <= {1 - user_config.threshold_nome}", "label_for_charts": f"Lev >= {user_config.threshold_nome}"},
            {"sql_condition": "ELSE", "label_for_charts": "Other"}
        ]
    })

    if passada == 1:
        # Passada 1: usa data de nascimento completa (Levenshtein) como comparison
        settings.comparisons.append({
            "output_column_name": "data_nascimento_std",
            "comparison_levels": [
                {"sql_condition": "data_nascimento_std_l IS NULL OR data_nascimento_std_r IS NULL", "label_for_charts": "Null", "is_null_level": True},
                {"sql_condition": f"levenshtein(data_nascimento_std_l, data_nascimento_std_r) / CAST(GREATEST(length(data_nascimento_std_l), length(data_nascimento_std_r)) AS FLOAT) <= {1 - user_config.threshold_data_nascimento}", "label_for_charts": f"Lev >= {user_config.threshold_data_nascimento}"},
                {"sql_condition": "ELSE", "label_for_charts": "Other"}
            ]
        })
    else:
        print(f"  ℹ PASSADA 2: Splink using nome_std only (no data_nascimento comparison)")

    # 3. Rodar Splink predict
    predict_threshold = user_config.threshold_match_probability_predict if passada == 1 else user_config.threshold_match_probability_predict_idade
    
    print(f"  Running Splink deduplication (threshold={predict_threshold})...")
    linker = Linker("master_dedup_block", settings, db_api=db_api)
    df_predictions = linker.inference.predict(
        threshold_match_probability=predict_threshold
    )
    
    monitor("02_splink_predict", grupo_str, block_count)

    # Materializar predições
    predictions_table = df_predictions.physical_name
    con.execute(f"""
        CREATE OR REPLACE TABLE temp_predictions_block AS
        SELECT * FROM {predictions_table}
    """)
    
    num_predictions = con.execute("SELECT COUNT(*) FROM temp_predictions_block").fetchone()[0]
    print(f"  Splink predictions: {num_predictions:,}")
    
    monitor_set_pares_gerados(num_predictions)
    monitor("03_predictions_materializadas", grupo_str, block_count, num_predictions)

    # 5. Criar pares_auditoria para este bloco
    # Query com scores dinâmicos baseados nos campos disponíveis
    query_pares = f"""
        CREATE OR REPLACE TABLE pares_auditoria_block AS
        SELECT 
            p.unique_id_l,
            p.unique_id_r,
            
            ml.fonte AS fonte_l,
            mr.fonte AS fonte_r,
            
            ml.nome_std AS nome_std_l,
            mr.nome_std AS nome_std_r,
            ml.nome_mae_std AS nome_mae_std_l,
            mr.nome_mae_std AS nome_mae_std_r,
            ml.nome_pai_std AS nome_pai_std_l,
            mr.nome_pai_std AS nome_pai_std_r,
            
            ml.primeiro_nome_std AS primeiro_nome_std_l,
            mr.primeiro_nome_std AS primeiro_nome_std_r,
            ml.primeiro_nome_mae_std AS primeiro_nome_mae_std_l,
            mr.primeiro_nome_mae_std AS primeiro_nome_mae_std_r,
            ml.primeiro_nome_pai_std AS primeiro_nome_pai_std_l,
            mr.primeiro_nome_pai_std AS primeiro_nome_pai_std_r,
            
            ml.data_nascimento_std AS data_nascimento_std_l,
            mr.data_nascimento_std AS data_nascimento_std_r,
            ml.ano_nascimento AS ano_nascimento_l,
            mr.ano_nascimento AS ano_nascimento_r,
            
            ml.sexo_std AS sexo_std_l,
            mr.sexo_std AS sexo_std_r,
            ml.origem_idade AS origem_idade_l,
            mr.origem_idade AS origem_idade_r,
            ml.ano_exato_idade AS ano_exato_idade_l,
            mr.ano_exato_idade AS ano_exato_idade_r,
            ml.unique_id_original AS unique_id_original_l,
            mr.unique_id_original AS unique_id_original_r,

            {sql_fragments['select_scores']}
            {sql_fragments['select_tamanhos']}
            
            CASE 
                WHEN p.match_probability IS NOT NULL AND p.match_probability >= {user_config.threshold_match_probability_cluster if passada == 1 else user_config.threshold_match_probability_cluster_idade} THEN 1 
                ELSE 0 
            END AS splink_decision,
            
            p.match_probability

        FROM temp_predictions_block p
        INNER JOIN master_dedup_block ml ON p.unique_id_l = ml.unique_id
        INNER JOIN master_dedup_block mr ON p.unique_id_r = mr.unique_id
        WHERE (
            -- Caso 1: Ambos com data real — nome >= threshold_nome (0.50)
            (
                ml.origem_idade = 0 AND mr.origem_idade = 0
                AND
                CASE 
                    WHEN CAST(ml.nome_std AS VARCHAR) IS NULL OR CAST(mr.nome_std AS VARCHAR) IS NULL THEN 0
                    ELSE 1.0 - (CAST(levenshtein(CAST(ml.nome_std AS VARCHAR), CAST(mr.nome_std AS VARCHAR)) AS FLOAT) / 
                            CAST(GREATEST(length(CAST(ml.nome_std AS VARCHAR)), length(CAST(mr.nome_std AS VARCHAR))) AS FLOAT))
                END >= {user_config.threshold_nome}
            )
            OR
            -- Caso 2: Pelo menos um sem data — nome >= 0.85
            (
                (ml.origem_idade = 1 OR mr.origem_idade = 1)
                AND
                CASE 
                    WHEN CAST(ml.nome_std AS VARCHAR) IS NULL OR CAST(mr.nome_std AS VARCHAR) IS NULL THEN 0
                    ELSE 1.0 - (CAST(levenshtein(CAST(ml.nome_std AS VARCHAR), CAST(mr.nome_std AS VARCHAR)) AS FLOAT) / 
                            CAST(GREATEST(length(CAST(ml.nome_std AS VARCHAR)), length(CAST(mr.nome_std AS VARCHAR))) AS FLOAT))
                END >= 0.85
            )
        )
        ORDER BY p.match_probability DESC NULLS LAST
    """
    
    con.execute(query_pares)
    
    # Converter scores para 0-100
    con.execute("""
        ALTER TABLE pares_auditoria_block ADD COLUMN nome_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN mae_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN pai_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN data_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_mae_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_pai_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_grudado_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_mae_grudado_score_100 FLOAT;
        ALTER TABLE pares_auditoria_block ADD COLUMN primeiro_nome_pai_grudado_score_100 FLOAT;
        
        UPDATE pares_auditoria_block SET
            nome_score_100 = COALESCE(score_nome * 100, 0),
            mae_score_100 = COALESCE(score_nome_mae * 100, 0),
            pai_score_100 = COALESCE(score_nome_pai * 100, 0),
            data_score_100 = COALESCE(score_data_nascimento * 100, 0),
            primeiro_nome_score_100 = score_primeiro_nome * 100,
            primeiro_nome_mae_score_100 = score_primeiro_nome_mae * 100,
            primeiro_nome_pai_score_100 = score_primeiro_nome_pai * 100,
            primeiro_nome_grudado_score_100 = score_primeiro_nome_grudado * 100,
            primeiro_nome_mae_grudado_score_100 = score_primeiro_nome_mae_grudado * 100,
            primeiro_nome_pai_grudado_score_100 = score_primeiro_nome_pai_grudado * 100
    """)
    
    # Deletar colunas de score originais
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_nome")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_nome_mae")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_nome_pai")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_data_nascimento")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_municipio")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome_mae")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome_pai")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome_grudado")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome_mae_grudado")
    con.execute("ALTER TABLE pares_auditoria_block DROP COLUMN score_primeiro_nome_pai_grudado")

    num_pares = con.execute("SELECT COUNT(*) FROM pares_auditoria_block").fetchone()[0]
    print(f"  Pairs after threshold filter: {num_pares:,}")
    
    monitor("04_pares_auditoria", grupo_str, block_count, num_pares)

    # 6. Aplicar refinamento se ativado
    if user_config.usar_decisao_refinada:
        print(f"  Applying refinement...")
        apply_refinement_block(user_config, con, linker, "pares_auditoria_block")
    
    # 7. Adicionar coluna de decisão final
    con.execute("ALTER TABLE pares_auditoria_block ADD COLUMN decisao_final INTEGER DEFAULT 0")    
    if user_config.usar_decisao_refinada:
            existing_cols = [row[0] for row in con.execute("DESCRIBE pares_auditoria_block").fetchall()]
            if 'decisao_refinada' in existing_cols:
                con.execute("""
                    UPDATE pares_auditoria_block 
                    SET decisao_final = COALESCE(decisao_refinada, 0)
                """)
            else:
                con.execute("""
                    UPDATE pares_auditoria_block 
                    SET decisao_final = 0
                """)    
    elif user_config.usar_splink:
        con.execute("""
            UPDATE pares_auditoria_block 
            SET decisao_final = splink_decision
        """)
    
    # Deduplicar triplicatas: manter melhor par por (unique_id_triplicado, unique_id_par)
    if tem_idade_no_bloco:
        triplicata_dupes = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT unique_id_l, unique_id_r, COUNT(*) as cnt
                FROM pares_auditoria_block
                WHERE decisao_final = 1 AND (origem_idade_l = 1 OR origem_idade_r = 1)
                GROUP BY unique_id_l, unique_id_r
                HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        
        if triplicata_dupes > 0:
            print(f"  Deduplicating triplicates: {triplicata_dupes:,} duplicate pairs found")
            
            # Marcar pares inferiores como decisao_final = 0
            # Critério: maior nome_score_100 vence; empate: ano_exato_idade vence
            con.execute("""
                UPDATE pares_auditoria_block
                SET decisao_final = 0
                WHERE decisao_final = 1
                AND (origem_idade_l = 1 OR origem_idade_r = 1)
                AND rowid NOT IN (
                    SELECT FIRST(rowid) FROM (
                        SELECT rowid, unique_id_l, unique_id_r, nome_score_100,
                                CASE 
                                    WHEN origem_idade_l = 1 AND ano_nascimento_l = ano_exato_idade_l THEN 1
                                    WHEN origem_idade_r = 1 AND ano_nascimento_r = ano_exato_idade_r THEN 1
                                    ELSE 0
                                END AS is_ano_exato
                        FROM pares_auditoria_block
                        WHERE decisao_final = 1
                            AND (origem_idade_l = 1 OR origem_idade_r = 1)
                        ORDER BY unique_id_l, unique_id_r, nome_score_100 DESC, is_ano_exato DESC
                    )
                    GROUP BY unique_id_l, unique_id_r
                )
            """)
            
            remaining = con.execute("""
                SELECT COUNT(*) FROM pares_auditoria_block 
                WHERE decisao_final = 1 AND (origem_idade_l = 1 OR origem_idade_r = 1)
            """).fetchone()[0]
            print(f"  After deduplication: {remaining:,} pairs remaining")

    # 7.5 Filtrar triplicatas: manter melhor par por unique_id_original
    tem_triplicatas = con.execute("""
        SELECT COUNT(*) FROM pares_auditoria_block 
        WHERE decisao_final = 1 
          AND (unique_id_original_l IS NOT NULL OR unique_id_original_r IS NOT NULL)
    """).fetchone()[0] > 0
    
    if tem_triplicatas:
        # Para cada unique_id_original, manter apenas o par com maior nome_score_100
        # Em caso de empate, manter o do ano exato da idade
        
        # Tratar lado L (quando o L é triplicata)
        con.execute("""
            UPDATE pares_auditoria_block
            SET decisao_final = 0
            WHERE decisao_final = 1
              AND unique_id_original_l IS NOT NULL
              AND unique_id_l NOT IN (
                  SELECT unique_id_l FROM (
                      SELECT unique_id_l, unique_id_original_l,
                             ROW_NUMBER() OVER (
                                 PARTITION BY unique_id_original_l
                                 ORDER BY nome_score_100 DESC,
                                          CASE WHEN ano_nascimento_l = ano_exato_idade_l THEN 0 ELSE 1 END ASC
                             ) AS rn
                      FROM pares_auditoria_block
                      WHERE decisao_final = 1 AND unique_id_original_l IS NOT NULL
                  ) ranked
                  WHERE rn = 1
              )
        """)
        
        # Tratar lado R (quando o R é triplicata)
        con.execute("""
            UPDATE pares_auditoria_block
            SET decisao_final = 0
            WHERE decisao_final = 1
              AND unique_id_original_r IS NOT NULL
              AND unique_id_r NOT IN (
                  SELECT unique_id_r FROM (
                      SELECT unique_id_r, unique_id_original_r,
                             ROW_NUMBER() OVER (
                                 PARTITION BY unique_id_original_r
                                 ORDER BY nome_score_100 DESC,
                                          CASE WHEN ano_nascimento_r = ano_exato_idade_r THEN 0 ELSE 1 END ASC
                             ) AS rn
                      FROM pares_auditoria_block
                      WHERE decisao_final = 1 AND unique_id_original_r IS NOT NULL
                  ) ranked
                  WHERE rn = 1
              )
        """)
        
        remaining = con.execute("""
            SELECT COUNT(*) FROM pares_auditoria_block WHERE decisao_final = 1
        """).fetchone()[0]
        print(f"  After triplicata filtering: {remaining:,} pairs remaining")

    # 8. Contar pares aprovados
    num_aprovados = con.execute("SELECT COUNT(*) FROM pares_auditoria_block WHERE decisao_final = 1").fetchone()[0]
    print(f"  Approved pairs: {num_aprovados:,}")
    
    # 9. Fazer append nas tabelas acumuladas
    print(f"  Appending results to accumulated tables...")
    
    # Adicionar coluna ano_bloco se não existir
    ano_bloco_id = min(anos)  # Usar menor ano do grupo como identificador
    con.execute(f"ALTER TABLE pares_auditoria_block ADD COLUMN ano_bloco INTEGER DEFAULT {ano_bloco_id}")
    
    # Garantir que todas as colunas necessárias existem antes do INSERT
    existing_cols = [row[0] for row in con.execute("DESCRIBE pares_auditoria_block").fetchall()]
    
    # Adicionar colunas que podem estar faltando
    optional_cols = {
        'decisao_refinada': 'INTEGER',
        'decision_reason': 'VARCHAR',
        'sobrenome_faltantes_pessoa': 'INTEGER',
        'sobrenome_trocados_pessoa': 'INTEGER',
        'sobrenome_faltantes_mae': 'INTEGER',
        'sobrenome_trocados_mae': 'INTEGER',
        'sobrenome_faltantes_pai': 'INTEGER',
        'sobrenome_trocados_pai': 'INTEGER',
    }
    
    for col, dtype in optional_cols.items():
        if col not in existing_cols:
            con.execute(f"ALTER TABLE pares_auditoria_block ADD COLUMN {col} {dtype} DEFAULT NULL")
    
    # Append pares_auditoria
    con.execute("""
        INSERT INTO pares_auditoria_acumulado 
        SELECT 
            unique_id_l, unique_id_r, fonte_l, fonte_r,
            nome_std_l, nome_std_r, nome_mae_std_l, nome_mae_std_r,
            nome_pai_std_l, nome_pai_std_r,
            primeiro_nome_std_l, primeiro_nome_std_r,
            primeiro_nome_mae_std_l, primeiro_nome_mae_std_r,
            primeiro_nome_pai_std_l, primeiro_nome_pai_std_r,
            data_nascimento_std_l, data_nascimento_std_r,
            ano_nascimento_l, ano_nascimento_r,
            sexo_std_l, sexo_std_r,
            nome_score_100, mae_score_100, pai_score_100, data_score_100,
            primeiro_nome_score_100,
            primeiro_nome_mae_score_100, primeiro_nome_pai_score_100,
            primeiro_nome_grudado_score_100, primeiro_nome_mae_grudado_score_100,
            primeiro_nome_pai_grudado_score_100,
            decisao_refinada, decisao_final,
            decision_reason,
            tamanho_primeiro_nome, tamanho_primeiro_nome_mae, tamanho_primeiro_nome_pai,
            origem_idade_l, origem_idade_r, ano_exato_idade_l, ano_exato_idade_r,
            unique_id_original_l, unique_id_original_r,
            ano_bloco,
            sobrenome_faltantes_pessoa, sobrenome_trocados_pessoa,
            sobrenome_faltantes_mae, sobrenome_trocados_mae,
            sobrenome_faltantes_pai, sobrenome_trocados_pai,
            match_probability
        FROM pares_auditoria_block
    """)

    # 10. Limpar tabelas temporárias do bloco
    print(f"  Cleaning up block tables...")
    con.execute("DROP TABLE IF EXISTS master_dedup_block")
    con.execute("DROP TABLE IF EXISTS temp_predictions_block")
    con.execute("DROP TABLE IF EXISTS all_candidate_pairs_block")
    con.execute("DROP TABLE IF EXISTS pares_auditoria_block")
    
    # Limpar memória Python
    del linker
    del df_predictions
    gc.collect()
    
    available_gb = get_available_memory_gb()
    print(f"  ✓ Group {grupo_str} complete. Available memory: {available_gb:.2f} GB")
    
    monitor_set_pares_aprovados(num_aprovados)
    monitor("05_bloco_finalizado", grupo_str, block_count, num_aprovados, is_final=True)

    return num_aprovados

def desambiguar_sim(user_config: UserConfig, con: duckdb.DuckDBPyConnection) -> None:
    """
    Resolve id_global com 2+ registros do SIM.
    Algoritmo de particionamento por propagação de afinidade nó a nó.
    Cada nó compara a aresta que o prende ao grupo atual vs a que o puxa para outro.
    Cascata: verifica vizinhos antes de migrar.
    Score de afinidade: média(nome_score_100, data_score_100). Empate: agrega mae e pai.
    
    Otimizado: busca todos os dados em batch (2 queries), processa em Python,
    devolve resultados em batch (2 UPDATEs via JOIN).
    """
    
    # Identificar clusters problemáticos
    clusters_problematicos = con.execute("""
        SELECT id_global
        FROM temp_clusters
        WHERE fonte = 'SIM' AND id_global IS NOT NULL
        GROUP BY id_global
        HAVING COUNT(*) >= 2
    """).fetchall()
    
    ids_problematicos = [row[0] for row in clusters_problematicos]
    print(f"  Processing {len(ids_problematicos)} clusters with multiple SIM records...")

    if not ids_problematicos:
        print(f"  ✓ SIM disambiguation complete: 0 clusters subdivided")
        return

    # ===== BATCH 1: buscar TODOS os nós e arestas de uma vez =====
    ph_clusters = ",".join([f"'{c}'" for c in ids_problematicos])
    
    # Todos os nós dos clusters problemáticos
    todos_nos = con.execute(f"""
        SELECT id_global, unique_id, fonte
        FROM temp_clusters
        WHERE id_global IN ({ph_clusters})
    """).fetchall()
    
    # Agrupar nós por cluster
    nos_por_cluster = {}  # {id_global: {unique_id: fonte}}
    uids_todos = set()
    for id_glob, uid, fonte in todos_nos:
        if id_glob not in nos_por_cluster:
            nos_por_cluster[id_glob] = {}
        nos_por_cluster[id_glob][uid] = fonte
        uids_todos.add(uid)
    
    # Todas as arestas aprovadas entre esses nós
    ph_uids = ",".join([f"'{u}'" for u in uids_todos])
    todas_arestas = con.execute(f"""
        SELECT unique_id_l, unique_id_r,
               COALESCE(nome_score_100, 0) AS nome_s,
               COALESCE(data_score_100, 0) AS data_s,
               COALESCE(mae_score_100, 0) AS mae_s,
               COALESCE(pai_score_100, 0) AS pai_s
        FROM pares_auditoria_acumulado
        WHERE decisao_final = 1
          AND unique_id_l IN ({ph_uids})
          AND unique_id_r IN ({ph_uids})
    """).fetchall()
    
    # Indexar arestas por par (para lookup rápido)
    arestas_idx = {}  # {(uid_l, uid_r): (nome_s, data_s, mae_s, pai_s)}
    for id_l, id_r, nome_s, data_s, mae_s, pai_s in todas_arestas:
        arestas_idx[(id_l, id_r)] = (nome_s, data_s, mae_s, pai_s)
        arestas_idx[(id_r, id_l)] = (nome_s, data_s, mae_s, pai_s)
    
    # ===== PROCESSAR CADA CLUSTER EM PYTHON (lógica idêntica à original) =====
    # Acumuladores de resultados batch
    updates_clusters = []    # [(unique_id, novo_id_global, sub_par_sim)]
    pares_cortados = []      # [(uid_l, uid_r)] para marcar cortado_desambiguacao_sim
    
    total_subdivisoes = 0
    
    for id_glob in ids_problematicos:
        nos = nos_por_cluster.get(id_glob, {})
        sims = [uid for uid, fonte in nos.items() if fonte == 'SIM']
        bases = [uid for uid, fonte in nos.items() if fonte != 'SIM']
        
        if len(sims) < 2:
            continue
        
        # Construir grafo local a partir do índice global
        uids = list(nos.keys())
        grafo = {uid: {} for uid in uids}
        
        for i, uid1 in enumerate(uids):
            for uid2 in uids[i+1:]:
                if (uid1, uid2) in arestas_idx:
                    nome_s, data_s, mae_s, pai_s = arestas_idx[(uid1, uid2)]
                    score_pri = (nome_s + data_s) / 2.0
                    score_des = (nome_s + data_s + mae_s + pai_s) / 4.0
                    grafo[uid1][uid2] = (score_pri, score_des)
                    grafo[uid2][uid1] = (score_pri, score_des)
        
        # --- Atribuição inicial ---
        atribuicao = {}
        
        for s in sims:
            atribuicao[s] = s
        
        for b in bases:
            melhor_sim = None
            melhor_score = (-1, -1)
            for s in sims:
                if s in grafo.get(b, {}):
                    sc = grafo[b][s]
                    if sc > melhor_score:
                        melhor_score = sc
                        melhor_sim = s
            if melhor_sim:
                atribuicao[b] = melhor_sim
        
        pendentes = [b for b in bases if b not in atribuicao]
        max_iter = 10
        while pendentes and max_iter > 0:
            novos_pendentes = []
            for b in pendentes:
                melhor_viz = None
                melhor_score = (-1, -1)
                for viz, sc in grafo.get(b, {}).items():
                    if viz in atribuicao and sc > melhor_score:
                        melhor_score = sc
                        melhor_viz = viz
                if melhor_viz:
                    atribuicao[b] = atribuicao[melhor_viz]
                else:
                    novos_pendentes.append(b)
            pendentes = novos_pendentes
            max_iter -= 1
        
        for b in pendentes:
            atribuicao[b] = None
        
        # --- Resolver fronteiras ---
        estavel = False
        max_iter_fronteira = 50
        iter_count = 0
        
        while not estavel and iter_count < max_iter_fronteira:
            estavel = True
            iter_count += 1
            
            fronteiras = []
            for uid, vizinhos in grafo.items():
                for viz, sc in vizinhos.items():
                    if uid < viz:
                        sim_uid = atribuicao.get(uid)
                        sim_viz = atribuicao.get(viz)
                        if sim_uid and sim_viz and sim_uid != sim_viz:
                            fronteiras.append((uid, viz, sc))
            
            if not fronteiras:
                break
            
            for uid, viz, sc_fronteira in fronteiras:
                sim_uid = atribuicao.get(uid)
                sim_viz = atribuicao.get(viz)
                if not sim_uid or not sim_viz or sim_uid == sim_viz:
                    continue
                
                ancora_uid = (-1, -1)
                for v, s in grafo.get(uid, {}).items():
                    if v != viz and atribuicao.get(v) == sim_uid:
                        if s > ancora_uid:
                            ancora_uid = s
                if sim_uid in grafo.get(uid, {}):
                    s_direta = grafo[uid][sim_uid]
                    if s_direta > ancora_uid:
                        ancora_uid = s_direta
                
                ancora_viz = (-1, -1)
                for v, s in grafo.get(viz, {}).items():
                    if v != uid and atribuicao.get(v) == sim_viz:
                        if s > ancora_viz:
                            ancora_viz = s
                if sim_viz in grafo.get(viz, {}):
                    s_direta = grafo[viz][sim_viz]
                    if s_direta > ancora_viz:
                        ancora_viz = s_direta
                
                puxar_uid = sc_fronteira
                puxar_viz = sc_fronteira
                
                uid_quer_migrar = puxar_uid > ancora_uid
                viz_quer_migrar = puxar_viz > ancora_viz
                
                if uid_quer_migrar and viz_quer_migrar:
                    score_sim_uid = grafo.get(uid, {}).get(sim_uid, (-1, -1))
                    score_sim_viz = grafo.get(viz, {}).get(sim_viz, (-1, -1))
                    if score_sim_viz >= score_sim_uid:
                        atribuicao[uid] = sim_viz
                        estavel = False
                    else:
                        atribuicao[viz] = sim_uid
                        estavel = False
                elif uid_quer_migrar:
                    atribuicao[uid] = sim_viz
                    estavel = False
                elif viz_quer_migrar:
                    atribuicao[viz] = sim_uid
                    estavel = False
                
                if not estavel:
                    migrou = uid if uid_quer_migrar or (uid_quer_migrar and viz_quer_migrar and score_sim_viz >= score_sim_uid) else viz if viz_quer_migrar else None
                    if migrou:
                        novo_sim = atribuicao[migrou]
                        antigo_sim = sim_uid if migrou == uid else sim_viz
                        for v, s in grafo.get(migrou, {}).items():
                            if atribuicao.get(v) == antigo_sim and v not in sims:
                                outra_ancora = (-1, -1)
                                for v2, s2 in grafo.get(v, {}).items():
                                    if v2 != migrou and atribuicao.get(v2) == antigo_sim:
                                        if s2 > outra_ancora:
                                            outra_ancora = s2
                                if antigo_sim in grafo.get(v, {}):
                                    s_direta = grafo[v][antigo_sim]
                                    if s_direta > outra_ancora:
                                        outra_ancora = s_direta
                                if s > outra_ancora:
                                    atribuicao[v] = novo_sim
        
        # --- Acumular resultados ---
        grupos = {}
        for uid, sim_attr in atribuicao.items():
            if sim_attr is None:
                continue
            if sim_attr not in grupos:
                grupos[sim_attr] = []
            grupos[sim_attr].append(uid)
        
        if len(grupos) > 1:
            total_subdivisoes += 1
            sub_idx = 1
            # Coletar membros de cada subgrupo para marcar pares cortados
            lista_grupos = list(grupos.values())
            for membros in grupos.values():
                novo_id = f"{id_glob}_{str(sub_idx).zfill(2)}"
                sub_idx += 1
                for m in membros:
                    updates_clusters.append((m, novo_id, 1))
            
            # Pares cortados: entre membros de subgrupos diferentes
            for i_g in range(len(lista_grupos)):
                for j_g in range(i_g + 1, len(lista_grupos)):
                    for m1 in lista_grupos[i_g]:
                        for m2 in lista_grupos[j_g]:
                            pares_cortados.append((m1, m2))
    
    # ===== BATCH 2: aplicar resultados da propagação no DuckDB =====
    if updates_clusters:
        df_updates = pl.DataFrame({
            'unique_id': [u[0] for u in updates_clusters],
            'novo_id_global': [u[1] for u in updates_clusters],
            'novo_sub_par_sim': [u[2] for u in updates_clusters]
        })
        con.register('temp_desambig_updates', df_updates.to_arrow())
        con.execute("""
            UPDATE temp_clusters
            SET id_global = upd.novo_id_global,
                SUB_PAR_SIM = upd.novo_sub_par_sim
            FROM temp_desambig_updates upd
            WHERE temp_clusters.unique_id = upd.unique_id
        """)
        con.unregister('temp_desambig_updates')
    
    if pares_cortados:
        df_cortados = pl.DataFrame({
            'uid_a': [p[0] for p in pares_cortados],
            'uid_b': [p[1] for p in pares_cortados]
        })
        con.register('temp_pares_cortados', df_cortados.to_arrow())
        con.execute("""
            UPDATE pares_auditoria_acumulado
            SET cortado_desambiguacao_sim = 1
            WHERE EXISTS (
                SELECT 1 FROM temp_pares_cortados c
                WHERE (pares_auditoria_acumulado.unique_id_l = c.uid_a AND pares_auditoria_acumulado.unique_id_r = c.uid_b)
                   OR (pares_auditoria_acumulado.unique_id_l = c.uid_b AND pares_auditoria_acumulado.unique_id_r = c.uid_a)
            )
        """)
        con.unregister('temp_pares_cortados')
    
    # ===== PASSO FINAL: forçar 1 SIM por grupo (também em batch) =====
    clusters_ainda_multi = con.execute("""
        SELECT id_global
        FROM temp_clusters
        WHERE fonte = 'SIM' AND id_global IS NOT NULL
        GROUP BY id_global
        HAVING COUNT(*) >= 2
    """).fetchall()
    
    ids_ainda_multi = [row[0] for row in clusters_ainda_multi]
    total_sim_removidos = 0
    
    if ids_ainda_multi:
        print(f"  Forcing 1 SIM per group: {len(ids_ainda_multi)} groups still have 2+ SIMs...")
        
        # Buscar todos os nós desses clusters em batch
        ph_multi = ",".join([f"'{c}'" for c in ids_ainda_multi])
        nos_multi = con.execute(f"""
            SELECT id_global, unique_id, fonte
            FROM temp_clusters
            WHERE id_global IN ({ph_multi})
        """).fetchall()
        
        nos_multi_por_cluster = {}
        for id_glob, uid, fonte in nos_multi:
            if id_glob not in nos_multi_por_cluster:
                nos_multi_por_cluster[id_glob] = {'sims': [], 'bases': []}
            if fonte == 'SIM':
                nos_multi_por_cluster[id_glob]['sims'].append(uid)
            else:
                nos_multi_por_cluster[id_glob]['bases'].append(uid)
        
        # Buscar scores em batch: todos os SIMs contra todas as bases desses clusters
        todos_sims_multi = []
        todos_bases_multi = []
        for info in nos_multi_por_cluster.values():
            todos_sims_multi.extend(info['sims'])
            todos_bases_multi.extend(info['bases'])
        
        scores_sim_base = {}  # {sim_id: {base_id: (nome, data, mae, pai)}}
        if todos_sims_multi and todos_bases_multi:
            ph_sims = ",".join([f"'{s}'" for s in todos_sims_multi])
            ph_bases = ",".join([f"'{b}'" for b in todos_bases_multi])
            rows_scores = con.execute(f"""
                SELECT unique_id_l, unique_id_r,
                       COALESCE(nome_score_100, 0) AS nome_s,
                       COALESCE(data_score_100, 0) AS data_s,
                       COALESCE(mae_score_100, 0) AS mae_s,
                       COALESCE(pai_score_100, 0) AS pai_s
                FROM pares_auditoria_acumulado
                WHERE decisao_final = 1
                  AND (
                      (unique_id_l IN ({ph_sims}) AND unique_id_r IN ({ph_bases}))
                      OR
                      (unique_id_r IN ({ph_sims}) AND unique_id_l IN ({ph_bases}))
                  )
            """).fetchall()
            
            for id_l, id_r, nome_s, data_s, mae_s, pai_s in rows_scores:
                # Determinar quem é SIM e quem é base
                sim_set = set(todos_sims_multi)
                if id_l in sim_set:
                    sim_id, base_id = id_l, id_r
                else:
                    sim_id, base_id = id_r, id_l
                if sim_id not in scores_sim_base:
                    scores_sim_base[sim_id] = {}
                scores_sim_base[sim_id][base_id] = (nome_s, data_s, mae_s, pai_s)
        
        # Processar cada cluster e acumular resultados
        sims_para_remover = []  # [(unique_id)]
        
        for id_glob, info in nos_multi_por_cluster.items():
            sims_ids = info['sims']
            bases_ids = info['bases']
            
            if len(sims_ids) < 2:
                continue
            
            scores_por_sim = {}
            for sim_id in sims_ids:
                if not bases_ids:
                    scores_por_sim[sim_id] = (0, 0)
                    continue
                
                # Calcular média dos scores contra as bases
                sim_scores = scores_sim_base.get(sim_id, {})
                nomes = [sim_scores[b][0] for b in bases_ids if b in sim_scores]
                datas = [sim_scores[b][1] for b in bases_ids if b in sim_scores]
                maes = [sim_scores[b][2] for b in bases_ids if b in sim_scores]
                pais = [sim_scores[b][3] for b in bases_ids if b in sim_scores]
                
                if nomes:
                    avg_nome = sum(nomes) / len(nomes)
                    avg_data = sum(datas) / len(datas)
                    avg_mae = sum(maes) / len(maes)
                    avg_pai = sum(pais) / len(pais)
                else:
                    avg_nome = avg_data = avg_mae = avg_pai = 0
                
                score_pri = (avg_nome + avg_data) / 2.0
                score_des = (avg_nome + avg_data + avg_mae + avg_pai) / 4.0
                scores_por_sim[sim_id] = (score_pri, score_des)
            
            sims_ordenados = sorted(scores_por_sim.keys(), key=lambda s: scores_por_sim[s], reverse=True)
            
            # Primeiro é o vencedor, resto são perdedores
            for sim_perdedor in sims_ordenados[1:]:
                sims_para_remover.append(sim_perdedor)
                total_sim_removidos += 1
        
        # Aplicar remoções em batch
        if sims_para_remover:
            df_remover = pl.DataFrame({'unique_id': sims_para_remover})
            con.register('temp_sims_remover', df_remover.to_arrow())
            
            con.execute("""
                UPDATE temp_clusters
                SET id_global = NULL, cluster_id = NULL, SUB_PAR_SIM = 1
                FROM temp_sims_remover r
                WHERE temp_clusters.unique_id = r.unique_id
            """)
            
            con.execute("""
                UPDATE pares_auditoria_acumulado
                SET cortado_desambiguacao_sim = 1
                WHERE decisao_final = 1
                  AND EXISTS (
                      SELECT 1 FROM temp_sims_remover r
                      WHERE pares_auditoria_acumulado.unique_id_l = r.unique_id
                         OR pares_auditoria_acumulado.unique_id_r = r.unique_id
                  )
            """)
            
            con.unregister('temp_sims_remover')
        
        print(f"  ✓ Removed {total_sim_removidos} duplicate SIM records from groups")

    print(f"  ✓ SIM disambiguation complete: {total_subdivisoes} clusters subdivided")

def consolidate_results(user_config: UserConfig, con: duckdb.DuckDBPyConnection) -> None:
    """
    Consolidate all block results: run Union-Find clustering and generate final outputs.
    """
    print("\n" + "="*60)
    print("CONSOLIDATING RESULTS FROM ALL BLOCKS")
    print("="*60)
    
    results_folder = Path(user_config.results_folder)
    
    # 1. Estatísticas gerais
    total_pares = con.execute("SELECT COUNT(*) FROM pares_auditoria_acumulado").fetchone()[0]
    total_aprovados = con.execute("SELECT COUNT(*) FROM pares_auditoria_acumulado WHERE decisao_final = 1").fetchone()[0]
    
    print(f"\nTotal pairs evaluated: {total_pares:,}")
    print(f"Total approved pairs: {total_aprovados:,}")
    
    if total_aprovados == 0:
        print("\n⚠ WARNING: No approved pairs found!")
        print("  Skipping clustering. Saving empty results...")
        
        # Salvar pares_auditoria vazio ou com o que tem
        audit_path = results_folder / "pares_auditoria.parquet"
        con.execute(f"COPY pares_auditoria_acumulado TO '{audit_path}' (FORMAT PARQUET)")
        print(f"  Saved: {audit_path}")
        return
    
    # 2. Clustering com Union-Find
    print("\nApplying Union-Find clustering...")
    
    pares_aprovados = con.execute("""
        SELECT DISTINCT unique_id_l, unique_id_r
        FROM pares_auditoria_acumulado
        WHERE decisao_final = 1
    """).fetchall()
    
    print(f"  Processing {len(pares_aprovados):,} approved pairs...")
    
    uf = UnionFind()
    for id_l, id_r in pares_aprovados:
        uf.union(id_l, id_r)
    
    clusters_dict = uf.get_clusters()
    
    num_clusters = len(set(clusters_dict.values()))
    print(f"  Found {num_clusters:,} unique clusters")
    print(f"  Covering {len(clusters_dict):,} unique IDs")
    
    # 3. Criar tabela de clusters
    df_clusters = pl.DataFrame({
    'unique_id': list(clusters_dict.keys()),
    'cluster_id': list(clusters_dict.values())
    })
    con.register('temp_clusters_pl', df_clusters.to_arrow())
    con.execute("CREATE OR REPLACE TABLE temp_clusters_final AS SELECT * FROM temp_clusters_pl")
    con.unregister('temp_clusters_pl')
    
    # 4. Gerar id_global sequencial único
    print("\nGenerating global IDs...")
    
    num_digits = max(2, len(str(num_clusters)))
    
    con.execute(f"""
        CREATE OR REPLACE TABLE cluster_id_mapping AS
        SELECT 
            cluster_id,
            'cluster_' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY cluster_id) AS VARCHAR), {num_digits}, '0') AS id_global
        FROM (SELECT DISTINCT cluster_id FROM temp_clusters_final)
    """)
    
    con.execute("""
        ALTER TABLE temp_clusters_final ADD COLUMN id_global VARCHAR;
        
        UPDATE temp_clusters_final
        SET id_global = (
            SELECT m.id_global
            FROM cluster_id_mapping m
            WHERE m.cluster_id = temp_clusters_final.cluster_id
        )
    """)
    
    # 5. Carregar base padronizada para gerar resultado final
    print("\nLoading standardized base for final results...")
    standardized_path = results_folder / "base_padronizada_pre_dedup.parquet"
    
    con.execute(f"""
        CREATE OR REPLACE TABLE master_dedup_final AS
        SELECT * FROM read_parquet('{standardized_path}')
    """)
    
    # 6. Criar temp_clusters com todos os registros
    con.execute("""
        CREATE OR REPLACE TABLE temp_clusters AS
        SELECT 
            m.unique_id,
            m.unique_id_original,
            m.fonte,
            m.nome_std,
            m.nome_mae_std,
            m.nome_pai_std,
            c.cluster_id,
            c.id_global,
            0 AS SUB_PAR_SIM
        FROM master_dedup_final m
        LEFT JOIN temp_clusters_final c ON m.unique_id = c.unique_id
    """)

    # 6.5. Desambiguação SIM: resolver id_global com 2+ registros do SIM
    print("\nChecking for id_global with multiple SIM records...")
    
    clusters_multi_sim = con.execute("""
        SELECT id_global, COUNT(*) as n_sim
        FROM temp_clusters
        WHERE fonte = 'SIM' AND id_global IS NOT NULL
        GROUP BY id_global
        HAVING COUNT(*) >= 2
    """).fetchall()
    
    con.execute("ALTER TABLE pares_auditoria_acumulado ADD COLUMN IF NOT EXISTS cortado_desambiguacao_sim INTEGER DEFAULT 0")

    if len(clusters_multi_sim) > 0:
        print(f"  Found {len(clusters_multi_sim)} id_global with 2+ SIM records. Resolving...")
        desambiguar_sim(user_config, con)
    else:
        print("  No id_global with multiple SIM records found. Skipping.")

    # 7. Atribuir id_global único para singletons
    print("Assigning unique id_global to singletons...")
    
    num_singletons = con.execute("""
        SELECT COUNT(*) FROM temp_clusters WHERE id_global IS NULL
    """).fetchone()[0]
    
    print(f"  Found {num_singletons:,} singletons")
    
    if num_singletons > 0:
        next_number = con.execute("""
            SELECT COALESCE(MAX(CAST(REPLACE(id_global, 'cluster_', '') AS INTEGER)), 0) + 1
            FROM temp_clusters
            WHERE id_global IS NOT NULL
        """).fetchone()[0]
        
        num_digits_total = max(num_digits, len(str(next_number + num_singletons)))
        
        con.execute(f"""
            CREATE OR REPLACE TABLE temp_singleton_ids AS
            SELECT 
                unique_id,
                'cluster_' || LPAD(
                    CAST({next_number} + ROW_NUMBER() OVER (ORDER BY unique_id) - 1 AS VARCHAR), 
                    {num_digits_total}, 
                    '0'
                ) AS new_id_global
            FROM temp_clusters
            WHERE id_global IS NULL
        """)
        
        con.execute("""
            UPDATE temp_clusters
            SET id_global = (
                SELECT new_id_global 
                FROM temp_singleton_ids 
                WHERE temp_singleton_ids.unique_id = temp_clusters.unique_id
            )
            WHERE id_global IS NULL
        """)
        
        print(f"  Assigned unique id_global to all singletons")
    
    # 8. Adicionar id_global ao pares_auditoria_acumulado
    print("\nAdding id_global to audit table...")

    con.execute("""
        ALTER TABLE pares_auditoria_acumulado ADD COLUMN IF NOT EXISTS id_global_l VARCHAR;
        ALTER TABLE pares_auditoria_acumulado ADD COLUMN IF NOT EXISTS id_global_r VARCHAR;
        
        UPDATE pares_auditoria_acumulado
        SET 
            id_global_l = (SELECT id_global FROM temp_clusters WHERE unique_id = pares_auditoria_acumulado.unique_id_l),
            id_global_r = (SELECT id_global FROM temp_clusters WHERE unique_id = pares_auditoria_acumulado.unique_id_r)
    """)
    
    # 9. Salvar arquivos finais em Parquet
    print("\nSaving final Parquet files...")
    
    # pares_auditoria.parquet
    audit_path = results_folder / "pares_auditoria.parquet"
    con.execute(f"COPY pares_auditoria_acumulado TO '{audit_path}' (FORMAT PARQUET)")
    print(f"  ✓ Saved: {audit_path}")
    
    # pares_c_match.parquet (apenas registros com cluster_id)
    clusters_path = results_folder / "pares_c_match.parquet"
    con.execute(f"""
        COPY (
            SELECT unique_id, unique_id_original, id_global, fonte, SUB_PAR_SIM
            FROM temp_clusters
            WHERE cluster_id IS NOT NULL
        ) TO '{clusters_path}' (FORMAT PARQUET)
    """)
    print(f"  ✓ Saved: {clusters_path}")
    
    # pares_todos.parquet
    con.execute("""
        CREATE OR REPLACE TABLE resultado_final AS
        SELECT 
            m.unique_id,
            m.unique_id_original,
            m.fonte,
            m.nome_std,
            m.nome_mae_std,
            m.nome_pai_std,
            c.cluster_id,
            c.id_global,
            CASE WHEN c.cluster_id IS NOT NULL THEN 1 ELSE 0 END AS have_match,
            c.SUB_PAR_SIM
        FROM master_dedup_final m
        LEFT JOIN temp_clusters c ON m.unique_id = c.unique_id
        ORDER BY c.id_global NULLS LAST, m.unique_id
    """)
    
    # Adicionar variáveis pareado e pareado_sim
    con.execute("""
        ALTER TABLE resultado_final ADD COLUMN IF NOT EXISTS pareado INTEGER DEFAULT 0;
        ALTER TABLE resultado_final ADD COLUMN IF NOT EXISTS pareado_sim INTEGER DEFAULT 0;
    """)
    con.execute("""
        UPDATE resultado_final
        SET pareado = CASE 
            WHEN id_global IN (
                SELECT id_global FROM resultado_final 
                GROUP BY id_global HAVING COUNT(*) > 1
            ) THEN 1 ELSE 0 END
    """)
    con.execute("""
        UPDATE resultado_final
        SET pareado_sim = CASE 
            WHEN id_global IN (
                SELECT DISTINCT id_global FROM resultado_final 
                WHERE fonte = 'SIM'
            ) THEN 1 ELSE 0 END
    """)

    final_path = results_folder / "pares_todos.parquet"
    con.execute(f"COPY resultado_final TO '{final_path}' (FORMAT PARQUET)")
    print(f"  ✓ Saved: {final_path}")
    
    # 10. Limpar tabelas temporárias
    print("\nCleaning up temporary tables...")
    con.execute("DROP TABLE IF EXISTS temp_clusters_final")
    con.execute("DROP TABLE IF EXISTS temp_clusters")
    con.execute("DROP TABLE IF EXISTS temp_singleton_ids")
    con.execute("DROP TABLE IF EXISTS cluster_id_mapping")
    con.execute("DROP TABLE IF EXISTS resultado_final")
    con.execute("DROP TABLE IF EXISTS master_dedup_final")
    
    gc.collect()
    
    print("\n✓ Consolidation complete!")

def part3_deduplicate(user_config: UserConfig, con: duckdb.DuckDBPyConnection) -> None:
    """
    PART 3: Run deduplication using Splink - BLOCK BY BLOCK processing.
    Processes each birth year separately to manage memory usage.
    """
    
    print("\n" + "="*60)
    print("PART 3: DEDUPLICATION (BLOCK-BY-BLOCK PROCESSING)")
    print("="*60)
    
    results_folder = Path(user_config.results_folder)
    standardized_path = results_folder / "base_padronizada_pre_dedup.parquet"
    
    if not standardized_path.exists():
        raise RuntimeError(f"Standardized file not found: {standardized_path}\nRun Part 1 and Part 2 first.")
    
    # 1. Limpar memória antes de começar
    print("\nCleaning memory before deduplication...")
    
    # Dropar tabelas que não precisamos mais
    tables = con.execute("SHOW TABLES").fetchall()
    for table in tables:
        table_name = table[0]
        if table_name not in ['master_dedup', 'pares_auditoria_acumulado']:
            print(f"  Dropping table: {table_name}")
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Dropar views também
    views = con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()
    for view in views:
        view_name = view[0]
        print(f"  Dropping view: {view_name}")
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
    
    gc.collect()
    available_gb = get_available_memory_gb()
    print(f"  Available memory: {available_gb:.2f} GB")
    
    # 2. Estimar memória por ano e agrupar
    estimates = estimate_memory_per_year(con, standardized_path)
    
    available_memory_mb = get_available_memory_gb() * 1024
    year_groups = group_years_by_memory(estimates, available_memory_mb, memory_percentage=0.75)
    
    # Verificar anos já processados no DuckDB (para retomada após interrupção)
    anos_ja_processados = set()
    try:
        resultado = con.execute("""
            SELECT DISTINCT ano_nascimento_l 
            FROM pares_auditoria_acumulado
        """).fetchall()
        anos_ja_processados = {row[0] for row in resultado}
        if anos_ja_processados:
            print(f"\n{'='*60}")
            print(f"RETOMADA DETECTADA")
            print(f"{'='*60}")
            print(f"  Anos já processados: {len(anos_ja_processados)}")
            print(f"  Lista: {sorted(anos_ja_processados)}")
    except:
        pass  # Tabela não existe ainda

    # Anos da Passada 2 ja concluidos (mesma logica da P1, filtrando os pares que
    # envolvem registro so-idade: a P1 so gera pares com origem_idade=0 nos dois
    # lados; a P2 sempre tem origem_idade=1 em pelo menos um lado).
    anos_p2_concluidos = set()
    try:
        resultado_p2 = con.execute("""
            SELECT DISTINCT ano_nascimento_l 
            FROM pares_auditoria_acumulado
            WHERE origem_idade_l = 1 OR origem_idade_r = 1
        """).fetchall()
        anos_p2_concluidos = {row[0] for row in resultado_p2}
    except:
        pass  # Tabela não existe ainda

    # Filtrar grupos para remover anos já processados
    if anos_ja_processados:
        year_groups_original = year_groups
        year_groups = []
        for group in year_groups_original:
            anos_pendentes = [ano for ano in group if ano not in anos_ja_processados]
            if anos_pendentes:
                year_groups.append(anos_pendentes)
        
        print(f"  Anos restantes a processar: {sum(len(g) for g in year_groups)}")
        print(f"  Grupos restantes: {len(year_groups)}")
        print(f"{'='*60}")

    # P2 completa = nao ha registros so-idade, ou todos os anos com idade ja estao
    # concluidos na P2. So fazemos a saida antecipada quando P1 E P2 estao completas;
    # caso contrario caimos no fluxo normal (loop da P1 vazio, loop da P2 retoma).
    _anos_com_idade = set()
    try:
        _res_idade = con.execute(f"""
            SELECT DISTINCT ano_nascimento FROM read_parquet('{standardized_path}')
            WHERE origem_idade = 1
        """).fetchall()
        _anos_com_idade = {row[0] for row in _res_idade}
    except:
        pass
    _p2_completa = (not _anos_com_idade) or _anos_com_idade.issubset(anos_p2_concluidos)

    # Se todos os anos já foram processados (P1 e P2), verificar se consolidação já foi feita
    if anos_ja_processados and not year_groups and _p2_completa:
        print(f"\n{'='*60}")
        print("TODOS OS BLOCOS JÁ PROCESSADOS!")
        print(f"{'='*60}")
        
        # Verificar se arquivos finais de consolidação já existem
        pares_c_match_path = results_folder / "pares_c_match.parquet"
        pares_todos_path = results_folder / "pares_todos.parquet"
        
        if pares_c_match_path.exists() and pares_todos_path.exists():
            print("  Arquivos de consolidação já existem:")
            print(f"    - {pares_c_match_path}")
            print(f"    - {pares_todos_path}")
            print("  Pulando para criar_bases_finais()...")
            return
        else:
            print("  Arquivos de consolidação NÃO existem. Executando consolidate_results()...")
            consolidate_results(user_config, con)
            return

    total_records = con.execute(f"SELECT COUNT(*) FROM read_parquet('{standardized_path}')").fetchone()[0]
    print(f"\nTotal records to process: {total_records:,}")
    
    # Verificar se existem registros com idade
    tem_registros_idade = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{standardized_path}') WHERE origem_idade = 1
    """).fetchone()[0] > 0
    
    # ================================================================
    # PASSADA 1: Deduplicação com nome + data de nascimento
    # ================================================================
    print("\n" + "="*60)
    print("PASSADA 1: DEDUPLICATION WITH BIRTH DATE")
    print("="*60)
    
    # 3. Inicializar tabelas de append
    print("\nInitializing append tables...")
    init_append_tables(con)
    
    # 4. Processar cada grupo de anos (apenas registros com data de nascimento)
    total_aprovados_p1 = 0
    grupos_processados_p1 = 0
    
    for group in year_groups:
        try:
            aprovados = process_year_block(user_config, con, group, standardized_path, passada=1)
            total_aprovados_p1 += aprovados
            grupos_processados_p1 += 1
        except Exception as e:
            grupo_str = f"{min(group)}-{max(group)}" if len(group) > 1 else str(group[0])
            print(f"\n⚠ ERROR processing group {grupo_str}: {e}")
            print(f"  Continuing with next group...")
            con.execute("DROP TABLE IF EXISTS master_dedup_block")
            con.execute("DROP TABLE IF EXISTS temp_predictions_block")
            con.execute("DROP TABLE IF EXISTS all_candidate_pairs_block")
            con.execute("DROP TABLE IF EXISTS pares_auditoria_block")
            gc.collect()
            continue
    
    print(f"\n{'='*60}")
    print(f"PASSADA 1 SUMMARY")
    print(f"{'='*60}")
    print(f"  Groups processed: {grupos_processados_p1}/{len(year_groups)}")
    print(f"  Total approved pairs (Passada 1): {total_aprovados_p1:,}")
    
    # ================================================================
    # PASSADA 2: Incorporação de registros com idade
    # ================================================================
    total_aprovados_p2 = 0
    
    if tem_registros_idade:
        print("\n" + "="*60)
        print("PASSADA 2: DEDUPLICATION WITH AGE-ONLY RECORDS")
        print("="*60)
        
        # Recalcular grupos de anos considerando TODOS os registros (idade inclusa)
        estimates_p2 = estimate_memory_per_year(con, standardized_path)
        available_memory_mb_p2 = get_available_memory_gb() * 1024
        year_groups_p2 = group_years_by_memory(estimates_p2, available_memory_mb_p2, memory_percentage=0.75)
        
        # Filtrar para manter apenas grupos que têm registros com idade
        anos_com_idade = set()
        result = con.execute(f"""
            SELECT DISTINCT ano_nascimento FROM read_parquet('{standardized_path}') WHERE origem_idade = 1
        """).fetchall()
        anos_com_idade = {row[0] for row in result}
        
        year_groups_p2 = [
            [ano for ano in group if ano in anos_com_idade and ano not in anos_p2_concluidos]
            for group in year_groups_p2
        ]
        year_groups_p2 = [g for g in year_groups_p2 if g]  # Remover grupos vazios (e anos ja concluidos na P2)
        
        print(f"\nYear groups with age-only records: {len(year_groups_p2)}")
        
        grupos_processados_p2 = 0
        
        for group in year_groups_p2:
            try:
                aprovados = process_year_block(user_config, con, group, standardized_path, passada=2)
                total_aprovados_p2 += aprovados
                grupos_processados_p2 += 1
            except Exception as e:
                grupo_str = f"{min(group)}-{max(group)}" if len(group) > 1 else str(group[0])
                print(f"\n⚠ ERROR processing group {grupo_str}: {e}")
                print(f"  Continuing with next group...")
                con.execute("DROP TABLE IF EXISTS master_dedup_block")
                con.execute("DROP TABLE IF EXISTS temp_predictions_block")
                con.execute("DROP TABLE IF EXISTS all_candidate_pairs_block")
                con.execute("DROP TABLE IF EXISTS pares_auditoria_block")
                gc.collect()
                continue
        
        print(f"\n{'='*60}")
        print(f"PASSADA 2 SUMMARY")
        print(f"{'='*60}")
        print(f"  Groups processed: {grupos_processados_p2}/{len(year_groups_p2)}")
        print(f"  Total approved pairs (Passada 2): {total_aprovados_p2:,}")
    
    print(f"\n{'='*60}")
    print(f"TOTAL SUMMARY")
    print(f"{'='*60}")
    print(f"  Total approved pairs: {total_aprovados_p1 + total_aprovados_p2:,}")
    
    # 5. Consolidar resultados
    consolidate_results(user_config, con)
    
    print(f"\n✓ Part 3 complete!")
    print(f"  Results saved in: {user_config.results_folder}/")

# ========================================================================================
# PART 4: TRUE PAIRS DECISION REFINEMENT - Decisions made by Dayan Oliveira manual refinement
# ========================================================================================
# Applies rule-based decision logic to refine Splink predictions, particularly for borderline
# pairs that fall between automatic accept/reject thresholds. Rules were developed through
# manual review of match patterns in Brazilian health databases.
#
# MAIN RESPONSIBILITIES:
# 1. FILTER BORDERLINE PAIRS: Select pairs between predict and cluster thresholds
# 2. APPLY DECISION RULES: Use avaliar_par_refinado() heuristics based on score patterns
# 3. RECLASSIFY PAIRS: Override Splink decisions for specific score combinations
# 4. GENERATE REFINED OUTPUT: Create pares_auditoria_block/pares_auditoria_acumulado with updated decisions

def calcular_sobrenome_overlap(user_config: UserConfig, con: duckdb.DuckDBPyConnection, table_name: str = "pares_auditoria_block") -> None:
    """
    Calcula a proporção de sobrenomes compartilhados entre pares usando DuckDB.
    
    Passo 1: Greedy matching via Levenshtein (tokens length > 1 dos dois lados)
    Passo 2: Tokens que sobraram — abreviações (length = 1) vs primeira letra de nomes completos
    
    Só calcula para pares na faixa 69.6-90 com preenchimento dos dois lados.
    Gera 3 colunas: sobrenome_ratio_pessoa, sobrenome_ratio_mae, sobrenome_ratio_pai
    """
    # Adicionar colunas de resultado
    for col in ['sobrenome_faltantes_pessoa', 'sobrenome_trocados_pessoa',
                'sobrenome_faltantes_mae', 'sobrenome_trocados_mae',
                'sobrenome_faltantes_pai', 'sobrenome_trocados_pai']:
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col} INTEGER DEFAULT NULL")
    
    def gerar_sql_sobrenome(campo_l, campo_r, coluna_score, coluna_faltantes, coluna_trocados,
                            max_iteracoes=10):
        """
        Executa cálculo de overlap de sobrenomes com iteração até estabilizar.
        
        Diferente da versão anterior (uma passada gulosa), agora itera até que
        nenhuma rodada produza novos matches, com teto de max_iteracoes (default 10).
        
        Resolve casos onde greedy míope descarta tokens que poderiam casar
        em uma segunda passada. Exemplo: "h d d moura" vs "hugo dias duarte moura":
          - Rodada 1: moura↔moura, h↔hugo, d↔dias (segundo d descartado pelo greedy)
          - Rodada 2: d↔duarte (agora sem disputa)
        """
        # Sufixo único para tabelas temporárias (evita colisão entre campos)
        sufixo = coluna_faltantes.replace("sobrenome_faltantes_", "")
        tt_pares = f"tt_pares_{sufixo}"
        tt_tokens_l = f"tt_tokens_l_{sufixo}"
        tt_tokens_r = f"tt_tokens_r_{sufixo}"
        tt_matches = f"tt_matches_{sufixo}"

        # ===== SETUP: extrai tokens e cria tabelas persistentes =====
        # Pares-alvo (apenas pares na faixa de score elegível)
        con.execute(f"DROP TABLE IF EXISTS {tt_pares}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_pares} AS
            SELECT unique_id_l, unique_id_r,
                   {campo_l} AS nome_l,
                   {campo_r} AS nome_r
            FROM {table_name}
            WHERE {coluna_score} >= {user_config.threshold_nome * 100}
              AND {coluna_score} <= 95
              AND {campo_l} IS NOT NULL
              AND {campo_r} IS NOT NULL
              AND TRIM({campo_l}) != ''
              AND TRIM({campo_r}) != ''
              AND length({campo_l}) - length(replace({campo_l}, ' ', '')) >= 1
              AND length({campo_r}) - length(replace({campo_r}, ' ', '')) >= 1
        """)

        # Tokens L: posição > 1 (exclui primeiro nome), filtra preposições
        con.execute(f"DROP TABLE IF EXISTS {tt_tokens_l}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_tokens_l} AS
            WITH pos AS (
                SELECT p.unique_id_l, p.unique_id_r,
                       unnest(string_split(p.nome_l, ' ')) AS token,
                       unnest(range(1, len(string_split(p.nome_l, ' ')) + 1)) AS pos
                FROM {tt_pares} p
            )
            SELECT unique_id_l, unique_id_r, token AS token_l, pos AS pos_l
            FROM pos
            WHERE pos > 1 AND length(token) > 0
              AND UPPER(token) NOT IN ('DE', 'DA', 'DO', 'DOS', 'DAS')
        """)

        # Tokens R: posição > 1, filtra preposições (espelha tokens_l)
        con.execute(f"DROP TABLE IF EXISTS {tt_tokens_r}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_tokens_r} AS
            WITH pos AS (
                SELECT p.unique_id_l, p.unique_id_r,
                       unnest(string_split(p.nome_r, ' ')) AS token,
                       unnest(range(1, len(string_split(p.nome_r, ' ')) + 1)) AS pos
                FROM {tt_pares} p
            )
            SELECT unique_id_l, unique_id_r, token AS token_r, pos AS pos_r
            FROM pos
            WHERE pos > 1 AND length(token) > 0
              AND UPPER(token) NOT IN ('DE', 'DA', 'DO', 'DOS', 'DAS')
        """)

        # Tabela de matches acumulados (vazia no início; cada rodada adiciona)
        con.execute(f"DROP TABLE IF EXISTS {tt_matches}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_matches} (
                unique_id_l VARCHAR,
                unique_id_r VARCHAR,
                pos_l INTEGER,
                pos_r INTEGER
            )
        """)

        # Contagem de preposições filtradas por lado (para heurística do 'd' órfão)
        tt_prep_l = f"tt_prep_l_{sufixo}"
        tt_prep_r = f"tt_prep_r_{sufixo}"
        con.execute(f"DROP TABLE IF EXISTS {tt_prep_l}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_prep_l} AS
            WITH pos AS (
                SELECT p.unique_id_l, p.unique_id_r,
                       unnest(string_split(p.nome_l, ' ')) AS token,
                       unnest(range(1, len(string_split(p.nome_l, ' ')) + 1)) AS pos
                FROM {tt_pares} p
            )
            SELECT unique_id_l, unique_id_r, COUNT(*) AS n_prep_l
            FROM pos
            WHERE pos > 1 AND length(token) > 0
              AND UPPER(token) IN ('DE', 'DA', 'DO', 'DOS', 'DAS')
            GROUP BY unique_id_l, unique_id_r
        """)
        con.execute(f"DROP TABLE IF EXISTS {tt_prep_r}")
        con.execute(f"""
            CREATE TEMP TABLE {tt_prep_r} AS
            WITH pos AS (
                SELECT p.unique_id_l, p.unique_id_r,
                       unnest(string_split(p.nome_r, ' ')) AS token,
                       unnest(range(1, len(string_split(p.nome_r, ' ')) + 1)) AS pos
                FROM {tt_pares} p
            )
            SELECT unique_id_l, unique_id_r, COUNT(*) AS n_prep_r
            FROM pos
            WHERE pos > 1 AND length(token) > 0
              AND UPPER(token) IN ('DE', 'DA', 'DO', 'DOS', 'DAS')
            GROUP BY unique_id_l, unique_id_r
        """)

        # ===== LOOP: iterar até estabilizar ou atingir max_iteracoes =====
        for iteracao in range(1, max_iteracoes + 1):
            # SQL único que faz Passo 1 (Lev) + Passo 2a + Passo 2b sobre as SOBRAS atuais
            # (tokens ainda não casados em iterações anteriores)
            sql_rodada = f"""
                INSERT INTO {tt_matches} (unique_id_l, unique_id_r, pos_l, pos_r)
                WITH sobras_l AS (
                    SELECT l.unique_id_l, l.unique_id_r, l.token_l, l.pos_l
                    FROM {tt_tokens_l} l
                    LEFT JOIN {tt_matches} m
                      ON l.unique_id_l = m.unique_id_l
                     AND l.unique_id_r = m.unique_id_r
                     AND l.pos_l = m.pos_l
                    WHERE m.pos_l IS NULL
                ),
                sobras_r AS (
                    SELECT r.unique_id_l, r.unique_id_r, r.token_r, r.pos_r
                    FROM {tt_tokens_r} r
                    LEFT JOIN {tt_matches} m
                      ON r.unique_id_l = m.unique_id_l
                     AND r.unique_id_r = m.unique_id_r
                     AND r.pos_r = m.pos_r
                    WHERE m.pos_r IS NULL
                ),
                -- ========== PASSO 1: Lev (tokens len > 1 dos dois lados) ==========
                cross_scores AS (
                    SELECT
                        l.unique_id_l, l.unique_id_r,
                        l.pos_l, r.pos_r,
                        LEAST(length(l.token_l), length(r.token_r)) AS min_len,
                        (1.0 - CAST(levenshtein(l.token_l, r.token_r) AS FLOAT)
                             / GREATEST(length(l.token_l), length(r.token_r))) * 100 AS lev_score
                    FROM sobras_l l
                    JOIN sobras_r r
                      ON l.unique_id_l = r.unique_id_l AND l.unique_id_r = r.unique_id_r
                    WHERE length(l.token_l) > 1 AND length(r.token_r) > 1
                ),
                scores_filtrados AS (
                    SELECT * FROM cross_scores
                    WHERE lev_score >= CASE WHEN min_len <= 3 THEN 65.0 ELSE 60.0 END
                ),
                best_per_l AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_l ORDER BY lev_score DESC, pos_r
                    ) AS rn_l
                    FROM scores_filtrados
                ),
                best_greedy AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_r ORDER BY lev_score DESC, pos_l
                    ) AS rn_r
                    FROM best_per_l WHERE rn_l = 1
                ),
                matches_lev AS (
                    SELECT unique_id_l, unique_id_r, pos_l, pos_r
                    FROM best_greedy WHERE rn_r = 1
                ),
                -- Sobras após Lev (para abreviação)
                sobras_l_apos_lev AS (
                    SELECT sl.* FROM sobras_l sl
                    LEFT JOIN matches_lev m
                      ON sl.unique_id_l = m.unique_id_l
                     AND sl.unique_id_r = m.unique_id_r
                     AND sl.pos_l = m.pos_l
                    WHERE m.pos_l IS NULL
                ),
                sobras_r_apos_lev AS (
                    SELECT sr.* FROM sobras_r sr
                    LEFT JOIN matches_lev m
                      ON sr.unique_id_l = m.unique_id_l
                     AND sr.unique_id_r = m.unique_id_r
                     AND sr.pos_r = m.pos_r
                    WHERE m.pos_r IS NULL
                ),
                -- ========== PASSO 2a: Abreviação L (len=1) vs R (len>1), mesma inicial ==========
                abrev_l_candidates AS (
                    SELECT sl.unique_id_l, sl.unique_id_r, sl.pos_l, sr.pos_r
                    FROM sobras_l_apos_lev sl
                    JOIN sobras_r_apos_lev sr
                      ON sl.unique_id_l = sr.unique_id_l AND sl.unique_id_r = sr.unique_id_r
                    WHERE length(sl.token_l) = 1 AND length(sr.token_r) > 1
                      AND sl.token_l = left(sr.token_r, 1)
                ),
                abrev_l_greedy1 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_l ORDER BY pos_r
                    ) AS rn_l
                    FROM abrev_l_candidates
                ),
                abrev_l_greedy2 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_r ORDER BY pos_l
                    ) AS rn_r
                    FROM abrev_l_greedy1 WHERE rn_l = 1
                ),
                matches_abrev_lr AS (
                    SELECT unique_id_l, unique_id_r, pos_l, pos_r
                    FROM abrev_l_greedy2 WHERE rn_r = 1
                ),
                -- Sobras após Lev + Abreviação L→R
                sobras_l_apos_abrev_lr AS (
                    SELECT sl.* FROM sobras_l_apos_lev sl
                    LEFT JOIN matches_abrev_lr m
                      ON sl.unique_id_l = m.unique_id_l
                     AND sl.unique_id_r = m.unique_id_r
                     AND sl.pos_l = m.pos_l
                    WHERE m.pos_l IS NULL
                ),
                sobras_r_apos_abrev_lr AS (
                    SELECT sr.* FROM sobras_r_apos_lev sr
                    LEFT JOIN matches_abrev_lr m
                      ON sr.unique_id_l = m.unique_id_l
                     AND sr.unique_id_r = m.unique_id_r
                     AND sr.pos_r = m.pos_r
                    WHERE m.pos_r IS NULL
                ),
                -- ========== PASSO 2b: Abreviação R (len=1) vs L (len>1), mesma inicial ==========
                abrev_r_candidates AS (
                    SELECT sl.unique_id_l, sl.unique_id_r, sl.pos_l, sr.pos_r
                    FROM sobras_r_apos_abrev_lr sr
                    JOIN sobras_l_apos_abrev_lr sl
                      ON sr.unique_id_l = sl.unique_id_l AND sr.unique_id_r = sl.unique_id_r
                    WHERE length(sr.token_r) = 1 AND length(sl.token_l) > 1
                      AND sr.token_r = left(sl.token_l, 1)
                ),
                abrev_r_greedy1 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_r ORDER BY pos_l
                    ) AS rn_r
                    FROM abrev_r_candidates
                ),
                abrev_r_greedy2 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_l ORDER BY pos_r
                    ) AS rn_l
                    FROM abrev_r_greedy1 WHERE rn_r = 1
                ),
                matches_abrev_rl AS (
                    SELECT unique_id_l, unique_id_r, pos_l, pos_r
                    FROM abrev_r_greedy2 WHERE rn_l = 1
                ),
                -- Sobras após Lev + Abreviação L→R + Abreviação R→L
                sobras_l_apos_abrev_rl AS (
                    SELECT sl.* FROM sobras_l_apos_abrev_lr sl
                    LEFT JOIN matches_abrev_rl m
                      ON sl.unique_id_l = m.unique_id_l
                     AND sl.unique_id_r = m.unique_id_r
                     AND sl.pos_l = m.pos_l
                    WHERE m.pos_l IS NULL
                ),
                sobras_r_apos_abrev_rl AS (
                    SELECT sr.* FROM sobras_r_apos_abrev_lr sr
                    LEFT JOIN matches_abrev_rl m
                      ON sr.unique_id_l = m.unique_id_l
                     AND sr.unique_id_r = m.unique_id_r
                     AND sr.pos_r = m.pos_r
                    WHERE m.pos_r IS NULL
                ),
                -- ========== PASSO 2c: Iniciais idênticas (L len=1 vs R len=1, mesmo caractere) ==========
                inicial_candidates AS (
                    SELECT sl.unique_id_l, sl.unique_id_r, sl.pos_l, sr.pos_r
                    FROM sobras_l_apos_abrev_rl sl
                    JOIN sobras_r_apos_abrev_rl sr
                      ON sl.unique_id_l = sr.unique_id_l AND sl.unique_id_r = sr.unique_id_r
                    WHERE length(sl.token_l) = 1 AND length(sr.token_r) = 1
                      AND sl.token_l = sr.token_r
                ),
                inicial_greedy1 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_l ORDER BY pos_r
                    ) AS rn_l
                    FROM inicial_candidates
                ),
                inicial_greedy2 AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY unique_id_l, unique_id_r, pos_r ORDER BY pos_l
                    ) AS rn_r
                    FROM inicial_greedy1 WHERE rn_l = 1
                ),
                matches_inicial AS (
                    SELECT unique_id_l, unique_id_r, pos_l, pos_r
                    FROM inicial_greedy2 WHERE rn_r = 1
                )
                -- Unir os 4 conjuntos de novos matches da rodada
                SELECT unique_id_l, unique_id_r, pos_l, pos_r FROM matches_lev
                UNION ALL
                SELECT unique_id_l, unique_id_r, pos_l, pos_r FROM matches_abrev_lr
                UNION ALL
                SELECT unique_id_l, unique_id_r, pos_l, pos_r FROM matches_abrev_rl
                UNION ALL
                SELECT unique_id_l, unique_id_r, pos_l, pos_r FROM matches_inicial
            """
            antes = con.execute(f"SELECT COUNT(*) FROM {tt_matches}").fetchone()[0]
            con.execute(sql_rodada)
            depois = con.execute(f"SELECT COUNT(*) FROM {tt_matches}").fetchone()[0]
            novos = depois - antes

            if novos == 0:
                # Convergiu: nenhuma rodada nova trouxe match
                break
        else:
            # Atingiu max_iteracoes sem convergir (pouco provável; loga só para auditoria)
            print(f"      [WARN] gerar_sql_sobrenome({sufixo}): atingiu max_iteracoes={max_iteracoes}")

        # ===== UPDATE FINAL: aplica n_faltantes e n_trocados na tabela principal =====
        # Inclui heurística do 'd' órfão: se sobrar token 'd' em um lado e houver
        # preposição (DE/DA/DO/DOS/DAS) filtrada no outro lado, desconta esse 'd'
        # da contagem de sobras (até min(d_orfaos, n_preposicoes_outro_lado)).
        con.execute(f"""
            UPDATE {table_name}
            SET {coluna_faltantes} = sub.n_faltantes,
                {coluna_trocados} = sub.n_trocados
            FROM (
                WITH contagem_tokens_l AS (
                    SELECT unique_id_l, unique_id_r, COUNT(*) AS n_tokens_l
                    FROM {tt_tokens_l} GROUP BY unique_id_l, unique_id_r
                ),
                contagem_tokens_r AS (
                    SELECT unique_id_l, unique_id_r, COUNT(*) AS n_tokens_r
                    FROM {tt_tokens_r} GROUP BY unique_id_l, unique_id_r
                ),
                total_matches AS (
                    SELECT unique_id_l, unique_id_r, COUNT(*) AS n_matches
                    FROM {tt_matches} GROUP BY unique_id_l, unique_id_r
                ),
                -- Tokens 'd' órfãos por lado (sobras de tokens cujo valor é 'd')
                d_orfaos_l AS (
                    SELECT l.unique_id_l, l.unique_id_r, COUNT(*) AS n_d_l
                    FROM {tt_tokens_l} l
                    LEFT JOIN {tt_matches} m
                      ON l.unique_id_l = m.unique_id_l
                     AND l.unique_id_r = m.unique_id_r
                     AND l.pos_l = m.pos_l
                    WHERE m.pos_l IS NULL
                      AND l.token_l = 'd'
                    GROUP BY l.unique_id_l, l.unique_id_r
                ),
                d_orfaos_r AS (
                    SELECT r.unique_id_l, r.unique_id_r, COUNT(*) AS n_d_r
                    FROM {tt_tokens_r} r
                    LEFT JOIN {tt_matches} m
                      ON r.unique_id_l = m.unique_id_l
                     AND r.unique_id_r = m.unique_id_r
                     AND r.pos_r = m.pos_r
                    WHERE m.pos_r IS NULL
                      AND r.token_r = 'd'
                    GROUP BY r.unique_id_l, r.unique_id_r
                ),
                ajustes AS (
                    SELECT cl.unique_id_l, cl.unique_id_r,
                           cl.n_tokens_l - COALESCE(m.n_matches, 0) AS restos_l_raw,
                           cr.n_tokens_r - COALESCE(m.n_matches, 0) AS restos_r_raw,
                           COALESCE(dl.n_d_l, 0) AS n_d_l,
                           COALESCE(dr.n_d_r, 0) AS n_d_r,
                           COALESCE(pl.n_prep_l, 0) AS n_prep_l,
                           COALESCE(pr.n_prep_r, 0) AS n_prep_r
                    FROM contagem_tokens_l cl
                    JOIN contagem_tokens_r cr
                      ON cl.unique_id_l = cr.unique_id_l AND cl.unique_id_r = cr.unique_id_r
                    LEFT JOIN total_matches m
                      ON cl.unique_id_l = m.unique_id_l AND cl.unique_id_r = m.unique_id_r
                    LEFT JOIN d_orfaos_l dl
                      ON cl.unique_id_l = dl.unique_id_l AND cl.unique_id_r = dl.unique_id_r
                    LEFT JOIN d_orfaos_r dr
                      ON cl.unique_id_l = dr.unique_id_l AND cl.unique_id_r = dr.unique_id_r
                    LEFT JOIN {tt_prep_l} pl
                      ON cl.unique_id_l = pl.unique_id_l AND cl.unique_id_r = pl.unique_id_r
                    LEFT JOIN {tt_prep_r} pr
                      ON cl.unique_id_l = pr.unique_id_l AND cl.unique_id_r = pr.unique_id_r
                )
                SELECT unique_id_l, unique_id_r,
                       ABS(
                           (restos_l_raw - LEAST(n_d_l, n_prep_r))
                           -
                           (restos_r_raw - LEAST(n_d_r, n_prep_l))
                       ) AS n_faltantes,
                       LEAST(
                           restos_l_raw - LEAST(n_d_l, n_prep_r),
                           restos_r_raw - LEAST(n_d_r, n_prep_l)
                       ) AS n_trocados
                FROM ajustes
            ) sub
            WHERE {table_name}.unique_id_l = sub.unique_id_l
              AND {table_name}.unique_id_r = sub.unique_id_r
        """)

        # ===== CLEANUP =====
        con.execute(f"DROP TABLE IF EXISTS {tt_pares}")
        con.execute(f"DROP TABLE IF EXISTS {tt_tokens_l}")
        con.execute(f"DROP TABLE IF EXISTS {tt_tokens_r}")
        con.execute(f"DROP TABLE IF EXISTS {tt_matches}")
        con.execute(f"DROP TABLE IF EXISTS {tt_prep_l}")
        con.execute(f"DROP TABLE IF EXISTS {tt_prep_r}")
    
    # Executar para pessoa, mãe e pai
    print("    Calculating surname overlap for nome...")
    gerar_sql_sobrenome('nome_std_l', 'nome_std_r', 'nome_score_100', 'sobrenome_faltantes_pessoa', 'sobrenome_trocados_pessoa')
    
    print("    Calculating surname overlap for mae...")
    gerar_sql_sobrenome('nome_mae_std_l', 'nome_mae_std_r', 'mae_score_100', 'sobrenome_faltantes_mae', 'sobrenome_trocados_mae')
    
    print("    Calculating surname overlap for pai...")
    gerar_sql_sobrenome('nome_pai_std_l', 'nome_pai_std_r', 'pai_score_100', 'sobrenome_faltantes_pai', 'sobrenome_trocados_pai')
    
    # Log
    for col_falt, col_troc, label in [
        ('sobrenome_faltantes_pessoa', 'sobrenome_trocados_pessoa', 'pessoa'),
        ('sobrenome_faltantes_mae', 'sobrenome_trocados_mae', 'mae'),
        ('sobrenome_faltantes_pai', 'sobrenome_trocados_pai', 'pai')
    ]:
        stats = con.execute(f"""
            SELECT COUNT({col_falt}) AS calculados,
                   AVG({col_falt}) AS media_falt,
                   AVG({col_troc}) AS media_troc
            FROM {table_name}
            WHERE {col_falt} IS NOT NULL
        """).fetchone()
        if stats[0] > 0:
            print(f"    Surname overlap {label}: {stats[0]:,} pairs, avg faltantes: {stats[1]:.2f}, avg trocados: {stats[2]:.2f}")


def apply_refinement_block(user_config: UserConfig, con: duckdb.DuckDBPyConnection, 
                           linker: Linker, table_name: str = "pares_auditoria_block") -> None:
    """Apply refinement via sequential filter pipeline (F1 → F2 → F3).
    
    Operates entirely in DuckDB SQL — no data leaves the database.
    Updates pares_auditoria_block with decisao_refinada and decision_reason.
    """
    
    print("  Applying refinement (filter pipeline)...")
    
    # Verificar se há pares
    total_pares = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    if total_pares == 0:
        print("  No pairs to refine")
        return
    
    print(f"  Total pairs entering pipeline: {total_pares:,}")
    
    # Adicionar colunas de trabalho e resultado
    con.execute(f"""
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS nome_score FLOAT;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS mae_score FLOAT;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS pai_score FLOAT;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS data_score FLOAT;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS decisao_refinada INTEGER DEFAULT 0;
        ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS decision_reason VARCHAR DEFAULT NULL;
    """)
    
    # Preparar scores de trabalho (equivalente ao fill_null(0))
    con.execute(f"""
        UPDATE {table_name} SET
            nome_score = COALESCE(nome_score_100, 0),
            mae_score = COALESCE(mae_score_100, 0),
            pai_score = COALESCE(pai_score_100, 0),
            data_score = COALESCE(data_score_100, 0)
    """)
    
    # Detectar se existe coluna origem_idade
    colunas = [row[0] for row in con.execute(f"DESCRIBE {table_name}").fetchall()]
    tem_coluna_idade = "origem_idade_l" in colunas
    
    # Condição base para fluxo principal (registros com data de nascimento)
    if tem_coluna_idade:
        filtro_principal = "AND origem_idade_l = 0 AND origem_idade_r = 0"
        filtro_idade = "AND (origem_idade_l = 1 OR origem_idade_r = 1)"
    else:
        filtro_principal = ""
        filtro_idade = ""
    
    # Thresholds do user_config (usados em F2 e F3, ambos fluxos)
    score_faixa_b_min = user_config.threshold_nome_faixa_b_min * 100
    score_faixa_c_min = user_config.threshold_nome_faixa_c_min * 100
    score_abreviacao = user_config.threshold_nome_abreviacao * 100
    
    th_a_3 = user_config.threshold_primnome_faixa_a_tam3
    th_a_5 = user_config.threshold_primnome_faixa_a_tam5
    th_a_8 = user_config.threshold_primnome_faixa_a_tam8
    th_a_max = user_config.threshold_primnome_faixa_a_tammax
    th_bc_3 = user_config.threshold_primnome_faixa_bc_tam3
    th_bc_5 = user_config.threshold_primnome_faixa_bc_tam5
    th_bc_8 = user_config.threshold_primnome_faixa_bc_tam8
    th_bc_max = user_config.threshold_primnome_faixa_bc_tammax
    
    max_sob_falt_pessoa = user_config.max_sobrenome_faltante_pessoa
    max_sob_troc_pessoa = user_config.max_sobrenome_trocado_pessoa
    max_sob_falt_pais = user_config.max_sobrenome_faltante_pais
    max_sob_troc_pais = user_config.max_sobrenome_trocado_pais
    th_grudado = user_config.threshold_sobrenome_grudado_total

    # Repescagem F3 (entidade-a-entidade): nome completo sem espaços, Levenshtein normalizado *100 >= th_grudado.
    # Guarda contra NULL e divisao por zero. So flipa reprovado->aprovado; nunca o contrario.
    def _resc_grudado(campo_l, campo_r):
        return (
            f"({campo_l} IS NOT NULL AND {campo_r} IS NOT NULL "
            f"AND length(replace(CAST({campo_l} AS VARCHAR),' ','')) > 0 "
            f"AND length(replace(CAST({campo_r} AS VARCHAR),' ','')) > 0 "
            f"AND (1.0 - CAST(levenshtein(replace(CAST({campo_l} AS VARCHAR),' ',''), replace(CAST({campo_r} AS VARCHAR),' ','')) AS FLOAT) "
            f"/ CAST(GREATEST(length(replace(CAST({campo_l} AS VARCHAR),' ','')), length(replace(CAST({campo_r} AS VARCHAR),' ',''))) AS FLOAT)) * 100 >= {th_grudado})"
        )
    resc_pessoa = _resc_grudado('nome_std_l', 'nome_std_r')
    resc_mae = _resc_grudado('nome_mae_std_l', 'nome_mae_std_r')
    resc_pai = _resc_grudado('nome_pai_std_l', 'nome_pai_std_r')
    
    # ================================================================
    # FLUXO PRINCIPAL (com data de nascimento)
    # ================================================================
    count_principal = con.execute(f"""
        SELECT COUNT(*) FROM {table_name} WHERE 1=1 {filtro_principal}
    """).fetchone()[0]
    
    if count_principal > 0:
        print(f"    Fluxo principal: {count_principal:,} pairs")
        
        # --- F1: Nome × Data × Pais ---
        # Marcar aprovados F1
        con.execute(f"""
            UPDATE {table_name}
            SET decisao_refinada = 1, decision_reason = 'Aprovado F1'
            WHERE decisao_refinada = 0 {filtro_principal}
            AND (
                -- Condição 1: nome∈[50,85), pais nulos ou <15, data==100
                (
                    nome_score >= 50 AND nome_score < 85
                    AND (mae_score IS NULL OR mae_score < 15)
                    AND (pai_score IS NULL OR pai_score < 15)
                    AND data_score = 100
                )
                OR
                -- Condição 2: nome∈[50,85), algum pai≥50, data≥85
                (
                    nome_score >= 50 AND nome_score < 85
                    AND (mae_score >= 50 OR pai_score >= 50)
                    AND data_score >= 85
                )
                OR
                -- Condição 3: nome≥85, data≥85
                (
                    nome_score >= 85 AND data_score >= 85
                )
            )
            -- Pré-requisito: mãe e pai NÃO podem estar em [15,50)
            AND NOT (mae_score IS NOT NULL AND mae_score >= 15 AND mae_score < 50)
            AND NOT (pai_score IS NOT NULL AND pai_score >= 15 AND pai_score < 50)
        """)
        
        # Marcar motivos de drop para quem NÃO passou F1
        con.execute(f"""
            UPDATE {table_name}
            SET decision_reason = CASE
                WHEN mae_score IS NOT NULL AND mae_score >= 15 AND mae_score < 50
                    THEN 'F1: mae_score>=15 e <50'
                WHEN pai_score IS NOT NULL AND pai_score >= 15 AND pai_score < 50
                    THEN 'F1: pai_score>=15 e <50'
                WHEN nome_score < 50
                    THEN 'F1: nome_score<50'
                WHEN nome_score >= 50 AND nome_score < 85
                    AND (mae_score IS NULL OR mae_score < 15)
                    AND (pai_score IS NULL OR pai_score < 15)
                    AND data_score < 100
                    THEN 'F1: nome em [50,85) e pais nulos/missing e data!=100'
                WHEN nome_score >= 50 AND nome_score < 85
                    AND (mae_score >= 50 OR pai_score >= 50)
                    AND data_score < 85
                    THEN 'F1: nome em [50,85) e pai/mae>=50 e data<85'
                WHEN nome_score >= 85 AND data_score < 85
                    THEN 'F1: nome>=85 e data<85'
                ELSE 'F1: motivo nao classificado'
            END
            WHERE decisao_refinada = 0 {filtro_principal}
            AND decision_reason IS NULL
        """)
        
        kept_f1 = con.execute(f"""
            SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_principal}
        """).fetchone()[0]
        dropped_f1 = count_principal - kept_f1
        print(f"    F1: {kept_f1:,} kept, {dropped_f1:,} dropped")
        
        # --- F2: Primeiro nome (pessoa, mãe, pai) ---
        if kept_f1 > 0:
            # Calcular sobrenome overlap para aprovados F1
            con.execute(f"""
                CREATE OR REPLACE TABLE temp_sobrenome_input AS
                SELECT t.* FROM {table_name} t
                WHERE t.decisao_refinada = 1 {filtro_principal}
            """)
            
            calcular_sobrenome_overlap(user_config, con, "temp_sobrenome_input")
            
            # Garantir que colunas de sobrenome existem na tabela principal
            for col_sob in ['sobrenome_faltantes_pessoa', 'sobrenome_trocados_pessoa',
                            'sobrenome_faltantes_mae', 'sobrenome_trocados_mae',
                            'sobrenome_faltantes_pai', 'sobrenome_trocados_pai']:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_sob} INTEGER DEFAULT NULL")
            
            # Trazer sobrenome de volta para tabela principal
            con.execute(f"""
                UPDATE {table_name}
                SET 
                    sobrenome_faltantes_pessoa = s.sobrenome_faltantes_pessoa,
                    sobrenome_trocados_pessoa = s.sobrenome_trocados_pessoa,
                    sobrenome_faltantes_mae = s.sobrenome_faltantes_mae,
                    sobrenome_trocados_mae = s.sobrenome_trocados_mae,
                    sobrenome_faltantes_pai = s.sobrenome_faltantes_pai,
                    sobrenome_trocados_pai = s.sobrenome_trocados_pai
                FROM temp_sobrenome_input s
                WHERE {table_name}.unique_id_l = s.unique_id_l
                AND {table_name}.unique_id_r = s.unique_id_r
            """)
            con.execute("DROP TABLE IF EXISTS temp_sobrenome_input")
                      
            # F2: Rejeitar por primeiro nome
            # Um par é rejeitado se qualquer campo (pessoa, mae, pai) tem primeiro nome
            # abaixo do threshold dinâmico e não é caso de abreviação
            con.execute(f"""
                UPDATE {table_name}
                SET decisao_refinada = 0,
                    decision_reason = CASE
                        -- Rejeita pessoa
                        WHEN (
                            COALESCE(tamanho_primeiro_nome, 0) > 0
                            AND COALESCE(primeiro_nome_score_100, -1) >= 0
                            AND COALESCE(primeiro_nome_score_100, -1) < (
                                CASE 
                                    WHEN nome_score < {score_faixa_b_min} THEN -- Faixa A
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE -- Faixa B/C
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND COALESCE(primeiro_nome_grudado_score_100, -1) < (
                                CASE 
                                    WHEN nome_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND NOT (
                                nome_score >= {score_abreviacao}
                                AND (
                                    (length(COALESCE(primeiro_nome_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_std_r,'')) > 1 AND COALESCE(primeiro_nome_std_l,'') = left(COALESCE(primeiro_nome_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_std_l,'')) > 1 AND COALESCE(primeiro_nome_std_r,'') = left(COALESCE(primeiro_nome_std_l,''), 1))
                                )
                            )
                        ) THEN 'F2: primeiro nome pessoa abaixo do threshold'
                        -- Rejeita mae
                        WHEN (
                            mae_score >= 15
                            AND COALESCE(tamanho_primeiro_nome_mae, 0) > 0
                            AND COALESCE(primeiro_nome_mae_score_100, -1) >= 0
                            AND COALESCE(primeiro_nome_mae_score_100, -1) < (
                                CASE 
                                    WHEN mae_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND COALESCE(primeiro_nome_mae_grudado_score_100, -1) < (
                                CASE 
                                    WHEN mae_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND NOT (
                                mae_score >= {score_abreviacao}
                                AND (
                                    (length(COALESCE(primeiro_nome_mae_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_r,'')) > 1 AND COALESCE(primeiro_nome_mae_std_l,'') = left(COALESCE(primeiro_nome_mae_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_mae_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_l,'')) > 1 AND COALESCE(primeiro_nome_mae_std_r,'') = left(COALESCE(primeiro_nome_mae_std_l,''), 1))
                                )
                            )
                        ) THEN 'F2: primeiro nome mae abaixo do threshold'
                        -- Rejeita pai
                        WHEN (
                            pai_score >= 15
                            AND COALESCE(tamanho_primeiro_nome_pai, 0) > 0
                            AND COALESCE(primeiro_nome_pai_score_100, -1) >= 0
                            AND COALESCE(primeiro_nome_pai_score_100, -1) < (
                                CASE 
                                    WHEN pai_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND COALESCE(primeiro_nome_pai_grudado_score_100, -1) < (
                                CASE 
                                    WHEN pai_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3}
                                             WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5}
                                             WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8}
                                             ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3}
                                             WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5}
                                             WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8}
                                             ELSE {th_bc_max} END
                                END
                            )
                            AND NOT (
                                pai_score >= {score_abreviacao}
                                AND (
                                    (length(COALESCE(primeiro_nome_pai_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_r,'')) > 1 AND COALESCE(primeiro_nome_pai_std_l,'') = left(COALESCE(primeiro_nome_pai_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_pai_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_l,'')) > 1 AND COALESCE(primeiro_nome_pai_std_r,'') = left(COALESCE(primeiro_nome_pai_std_l,''), 1))
                                )
                            )
                        ) THEN 'F2: primeiro nome pai abaixo do threshold'
                        ELSE NULL
                    END
                WHERE decisao_refinada = 1 {filtro_principal}
                AND (
                    -- Pessoa
                    (
                        COALESCE(tamanho_primeiro_nome, 0) > 0
                        AND COALESCE(primeiro_nome_score_100, -1) >= 0
                        AND COALESCE(primeiro_nome_score_100, -1) < (
                            CASE 
                                WHEN nome_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND COALESCE(primeiro_nome_grudado_score_100, -1) < (
                            CASE 
                                WHEN nome_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND NOT (
                            nome_score >= {score_abreviacao}
                            AND (
                                (length(COALESCE(primeiro_nome_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_std_r,'')) > 1 AND COALESCE(primeiro_nome_std_l,'') = left(COALESCE(primeiro_nome_std_r,''), 1))
                                OR (length(COALESCE(primeiro_nome_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_std_l,'')) > 1 AND COALESCE(primeiro_nome_std_r,'') = left(COALESCE(primeiro_nome_std_l,''), 1))
                            )
                        )
                    )
                    OR
                    -- Mae
                    (
                        mae_score >= 15
                        AND COALESCE(tamanho_primeiro_nome_mae, 0) > 0
                        AND COALESCE(primeiro_nome_mae_score_100, -1) >= 0
                        AND COALESCE(primeiro_nome_mae_score_100, -1) < (
                            CASE 
                                WHEN mae_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND COALESCE(primeiro_nome_mae_grudado_score_100, -1) < (
                            CASE 
                                WHEN mae_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND NOT (
                            mae_score >= {score_abreviacao}
                            AND (
                                (length(COALESCE(primeiro_nome_mae_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_r,'')) > 1 AND COALESCE(primeiro_nome_mae_std_l,'') = left(COALESCE(primeiro_nome_mae_std_r,''), 1))
                                OR (length(COALESCE(primeiro_nome_mae_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_l,'')) > 1 AND COALESCE(primeiro_nome_mae_std_r,'') = left(COALESCE(primeiro_nome_mae_std_l,''), 1))
                            )
                        )
                    )
                    OR
                    -- Pai
                    (
                        pai_score >= 15
                        AND COALESCE(tamanho_primeiro_nome_pai, 0) > 0
                        AND COALESCE(primeiro_nome_pai_score_100, -1) >= 0
                        AND COALESCE(primeiro_nome_pai_score_100, -1) < (
                            CASE 
                                WHEN pai_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND COALESCE(primeiro_nome_pai_grudado_score_100, -1) < (
                            CASE 
                                WHEN pai_score < {score_faixa_b_min} THEN
                                    CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3}
                                         WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5}
                                         WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8}
                                         ELSE {th_a_max} END
                                ELSE
                                    CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3}
                                         WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5}
                                         WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8}
                                         ELSE {th_bc_max} END
                            END
                        )
                        AND NOT (
                            pai_score >= {score_abreviacao}
                            AND (
                                (length(COALESCE(primeiro_nome_pai_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_r,'')) > 1 AND COALESCE(primeiro_nome_pai_std_l,'') = left(COALESCE(primeiro_nome_pai_std_r,''), 1))
                                OR (length(COALESCE(primeiro_nome_pai_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_l,'')) > 1 AND COALESCE(primeiro_nome_pai_std_r,'') = left(COALESCE(primeiro_nome_pai_std_l,''), 1))
                            )
                        )
                    )
                )
            """)
            
            kept_f2 = con.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_principal}
            """).fetchone()[0]
            dropped_f2 = kept_f1 - kept_f2
            print(f"    F2: {kept_f2:,} kept, {dropped_f2:,} dropped")
        
        # --- F3: Sobrenomes ---
        kept_after_f2 = con.execute(f"""
            SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_principal}
        """).fetchone()[0]
        
        if kept_after_f2 > 0:  
            con.execute(f"""
                UPDATE {table_name}
                SET decisao_refinada = 0,
                    decision_reason = CASE
                        -- Pessoa
                        WHEN sobrenome_faltantes_pessoa IS NOT NULL AND NOT {resc_pessoa} AND (
                            (nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pessoa > 0 OR sobrenome_trocados_pessoa > 0))
                            OR
                            (nome_score >= {score_faixa_b_min} AND nome_score < {score_faixa_c_min} AND (sobrenome_faltantes_pessoa > {max_sob_falt_pessoa} OR sobrenome_trocados_pessoa > {max_sob_troc_pessoa}))
                        ) THEN 'F3: sobrenome pessoa insuficiente'
                        -- Mae
                        WHEN sobrenome_faltantes_mae IS NOT NULL AND NOT {resc_mae} AND (
                            (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_mae > 0 OR sobrenome_trocados_mae > 0))
                            OR
                            (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                            OR
                            (mae_score >= {score_faixa_b_min} AND mae_score < {score_faixa_c_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                        ) THEN 'F3: sobrenome mae insuficiente'
                        -- Pai
                        WHEN sobrenome_faltantes_pai IS NOT NULL AND NOT {resc_pai} AND (
                            (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pai > 0 OR sobrenome_trocados_pai > 0))
                            OR
                            (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                            OR
                            (pai_score >= {score_faixa_b_min} AND pai_score < {score_faixa_c_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                        ) THEN 'F3: sobrenome pai insuficiente'
                        ELSE NULL
                    END
                WHERE decisao_refinada = 1 {filtro_principal}
                AND (
                    -- Pessoa insuficiente
                    (sobrenome_faltantes_pessoa IS NOT NULL AND NOT {resc_pessoa} AND (
                        (nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pessoa > 0 OR sobrenome_trocados_pessoa > 0))
                        OR (nome_score >= {score_faixa_b_min} AND nome_score < {score_faixa_c_min} AND (sobrenome_faltantes_pessoa > {max_sob_falt_pessoa} OR sobrenome_trocados_pessoa > {max_sob_troc_pessoa}))
                    ))
                    OR
                    -- Mae insuficiente
                    (sobrenome_faltantes_mae IS NOT NULL AND NOT {resc_mae} AND (
                        (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_mae > 0 OR sobrenome_trocados_mae > 0))
                        OR (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                        OR (mae_score >= {score_faixa_b_min} AND mae_score < {score_faixa_c_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                    ))
                    OR
                    -- Pai insuficiente
                    (sobrenome_faltantes_pai IS NOT NULL AND NOT {resc_pai} AND (
                        (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pai > 0 OR sobrenome_trocados_pai > 0))
                        OR (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                        OR (pai_score >= {score_faixa_b_min} AND pai_score < {score_faixa_c_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                    ))
                )
            """)
            
            kept_f3 = con.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_principal}
            """).fetchone()[0]
            dropped_f3 = kept_after_f2 - kept_f3
            print(f"    F3: {kept_f3:,} kept, {dropped_f3:,} dropped")
    
    # ================================================================
    # FLUXO IDADE MODE (sem data de nascimento)
    # ================================================================
    if tem_coluna_idade:
        count_idade = con.execute(f"""
            SELECT COUNT(*) FROM {table_name} WHERE 1=1 {filtro_idade}
        """).fetchone()[0]
        
        if count_idade > 0:
            print(f"    Fluxo IDADE MODE: {count_idade:,} pairs")
            
            # --- F1-ID: Nome × Pais (sem data) ---
            con.execute(f"""
                UPDATE {table_name}
                SET decisao_refinada = 1, decision_reason = 'Aprovado F1-ID'
                WHERE decisao_refinada = 0 {filtro_idade}
                AND (
                    -- Condição 1: nome∈[50,85), algum pai≥50
                    (
                        nome_score >= 50 AND nome_score < 85
                        AND (mae_score >= 50 OR pai_score >= 50)
                    )
                    OR
                    -- Condição 2: nome≥85, pais nulos ou <15
                    (
                        nome_score >= 85
                        AND (mae_score IS NULL OR mae_score < 15)
                        AND (pai_score IS NULL OR pai_score < 15)
                    )
                    OR
                    -- Condição 3: nome≥85, algum pai≥50
                    (
                        nome_score >= 85
                        AND (mae_score >= 50 OR pai_score >= 50)
                    )
                )
                AND NOT (mae_score IS NOT NULL AND mae_score >= 15 AND mae_score < 50)
                AND NOT (pai_score IS NOT NULL AND pai_score >= 15 AND pai_score < 50)
            """)
            
            # Motivos de drop F1-ID
            con.execute(f"""
                UPDATE {table_name}
                SET decision_reason = CASE
                    WHEN mae_score IS NOT NULL AND mae_score >= 15 AND mae_score < 50
                        THEN 'F1-ID: mae_score>=15 e <50'
                    WHEN pai_score IS NOT NULL AND pai_score >= 15 AND pai_score < 50
                        THEN 'F1-ID: pai_score>=15 e <50'
                    WHEN nome_score < 50
                        THEN 'F1-ID: nome_score<50'
                    WHEN nome_score >= 50 AND nome_score < 85
                        AND (mae_score IS NULL OR mae_score < 15)
                        AND (pai_score IS NULL OR pai_score < 15)
                        THEN 'F1-ID: nome em [50,85) e pais nulos/missing'
                    ELSE 'F1-ID: motivo nao classificado'
                END
                WHERE decisao_refinada = 0 {filtro_idade}
                AND decision_reason IS NULL
            """)
            
            kept_f1id = con.execute(f"""
                SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_idade}
            """).fetchone()[0]
            dropped_f1id = count_idade - kept_f1id
            print(f"    F1-ID: {kept_f1id:,} kept, {dropped_f1id:,} dropped")
            
            # --- F2-ID e F3-ID: mesma lógica do fluxo principal ---
            if kept_f1id > 0:
                con.execute(f"""
                    CREATE OR REPLACE TABLE temp_sobrenome_input_id AS
                    SELECT t.* FROM {table_name} t
                    WHERE t.decisao_refinada = 1 {filtro_idade}
                """)
                
                calcular_sobrenome_overlap(user_config, con, "temp_sobrenome_input_id")
                
                # Garantir que colunas de sobrenome existem na tabela principal
                for col_sob in ['sobrenome_faltantes_pessoa', 'sobrenome_trocados_pessoa',
                                'sobrenome_faltantes_mae', 'sobrenome_trocados_mae',
                                'sobrenome_faltantes_pai', 'sobrenome_trocados_pai']:
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS {col_sob} INTEGER DEFAULT NULL")
                
                con.execute(f"""
                    UPDATE {table_name}
                    SET 
                        sobrenome_faltantes_pessoa = s.sobrenome_faltantes_pessoa,
                        sobrenome_trocados_pessoa = s.sobrenome_trocados_pessoa,
                        sobrenome_faltantes_mae = s.sobrenome_faltantes_mae,
                        sobrenome_trocados_mae = s.sobrenome_trocados_mae,
                        sobrenome_faltantes_pai = s.sobrenome_faltantes_pai,
                        sobrenome_trocados_pai = s.sobrenome_trocados_pai
                    FROM temp_sobrenome_input_id s
                    WHERE {table_name}.unique_id_l = s.unique_id_l
                    AND {table_name}.unique_id_r = s.unique_id_r
                """)
                con.execute("DROP TABLE IF EXISTS temp_sobrenome_input_id")
                
                # F2-ID (mesmos thresholds já definidos acima)
                con.execute(f"""
                    UPDATE {table_name}
                    SET decisao_refinada = 0,
                        decision_reason = CASE
                            WHEN (
                                COALESCE(tamanho_primeiro_nome, 0) > 0
                                AND COALESCE(primeiro_nome_score_100, -1) >= 0
                                AND COALESCE(primeiro_nome_score_100, -1) < (
                                    CASE WHEN nome_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND COALESCE(primeiro_nome_grudado_score_100, -1) < (
                                    CASE WHEN nome_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND NOT (nome_score >= {score_abreviacao} AND (
                                    (length(COALESCE(primeiro_nome_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_std_r,'')) > 1 AND COALESCE(primeiro_nome_std_l,'') = left(COALESCE(primeiro_nome_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_std_l,'')) > 1 AND COALESCE(primeiro_nome_std_r,'') = left(COALESCE(primeiro_nome_std_l,''), 1))))
                            ) THEN 'F2-ID: primeiro nome pessoa abaixo do threshold'
                            WHEN (
                                mae_score >= 15 AND COALESCE(tamanho_primeiro_nome_mae, 0) > 0
                                AND COALESCE(primeiro_nome_mae_score_100, -1) >= 0
                                AND COALESCE(primeiro_nome_mae_score_100, -1) < (
                                    CASE WHEN mae_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND COALESCE(primeiro_nome_mae_grudado_score_100, -1) < (
                                    CASE WHEN mae_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND NOT (mae_score >= {score_abreviacao} AND (
                                    (length(COALESCE(primeiro_nome_mae_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_r,'')) > 1 AND COALESCE(primeiro_nome_mae_std_l,'') = left(COALESCE(primeiro_nome_mae_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_mae_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_l,'')) > 1 AND COALESCE(primeiro_nome_mae_std_r,'') = left(COALESCE(primeiro_nome_mae_std_l,''), 1))))
                            ) THEN 'F2-ID: primeiro nome mae abaixo do threshold'
                            WHEN (
                                pai_score >= 15 AND COALESCE(tamanho_primeiro_nome_pai, 0) > 0
                                AND COALESCE(primeiro_nome_pai_score_100, -1) >= 0
                                AND COALESCE(primeiro_nome_pai_score_100, -1) < (
                                    CASE WHEN pai_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND COALESCE(primeiro_nome_pai_grudado_score_100, -1) < (
                                    CASE WHEN pai_score < {score_faixa_b_min} THEN
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8} ELSE {th_a_max} END
                                    ELSE
                                        CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8} ELSE {th_bc_max} END
                                    END)
                                AND NOT (pai_score >= {score_abreviacao} AND (
                                    (length(COALESCE(primeiro_nome_pai_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_r,'')) > 1 AND COALESCE(primeiro_nome_pai_std_l,'') = left(COALESCE(primeiro_nome_pai_std_r,''), 1))
                                    OR (length(COALESCE(primeiro_nome_pai_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_l,'')) > 1 AND COALESCE(primeiro_nome_pai_std_r,'') = left(COALESCE(primeiro_nome_pai_std_l,''), 1))))
                            ) THEN 'F2-ID: primeiro nome pai abaixo do threshold'
                            ELSE NULL
                        END
                    WHERE decisao_refinada = 1 {filtro_idade}
                    AND (
                        (COALESCE(tamanho_primeiro_nome, 0) > 0 AND COALESCE(primeiro_nome_score_100, -1) >= 0
                         AND COALESCE(primeiro_nome_score_100, -1) < (CASE WHEN nome_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND COALESCE(primeiro_nome_grudado_score_100, -1) < (CASE WHEN nome_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND NOT (nome_score >= {score_abreviacao} AND ((length(COALESCE(primeiro_nome_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_std_r,'')) > 1 AND COALESCE(primeiro_nome_std_l,'') = left(COALESCE(primeiro_nome_std_r,''), 1)) OR (length(COALESCE(primeiro_nome_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_std_l,'')) > 1 AND COALESCE(primeiro_nome_std_r,'') = left(COALESCE(primeiro_nome_std_l,''), 1)))))
                        OR (mae_score >= 15 AND COALESCE(tamanho_primeiro_nome_mae, 0) > 0 AND COALESCE(primeiro_nome_mae_score_100, -1) >= 0
                         AND COALESCE(primeiro_nome_mae_score_100, -1) < (CASE WHEN mae_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND COALESCE(primeiro_nome_mae_grudado_score_100, -1) < (CASE WHEN mae_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome_mae <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_mae <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_mae <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND NOT (mae_score >= {score_abreviacao} AND ((length(COALESCE(primeiro_nome_mae_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_r,'')) > 1 AND COALESCE(primeiro_nome_mae_std_l,'') = left(COALESCE(primeiro_nome_mae_std_r,''), 1)) OR (length(COALESCE(primeiro_nome_mae_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_mae_std_l,'')) > 1 AND COALESCE(primeiro_nome_mae_std_r,'') = left(COALESCE(primeiro_nome_mae_std_l,''), 1)))))
                        OR (pai_score >= 15 AND COALESCE(tamanho_primeiro_nome_pai, 0) > 0 AND COALESCE(primeiro_nome_pai_score_100, -1) >= 0
                         AND COALESCE(primeiro_nome_pai_score_100, -1) < (CASE WHEN pai_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND COALESCE(primeiro_nome_pai_grudado_score_100, -1) < (CASE WHEN pai_score < {score_faixa_b_min} THEN CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_a_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_a_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_a_8} ELSE {th_a_max} END ELSE CASE WHEN tamanho_primeiro_nome_pai <= 3 THEN {th_bc_3} WHEN tamanho_primeiro_nome_pai <= 5 THEN {th_bc_5} WHEN tamanho_primeiro_nome_pai <= 8 THEN {th_bc_8} ELSE {th_bc_max} END END)
                         AND NOT (pai_score >= {score_abreviacao} AND ((length(COALESCE(primeiro_nome_pai_std_l,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_r,'')) > 1 AND COALESCE(primeiro_nome_pai_std_l,'') = left(COALESCE(primeiro_nome_pai_std_r,''), 1)) OR (length(COALESCE(primeiro_nome_pai_std_r,'')) = 1 AND length(COALESCE(primeiro_nome_pai_std_l,'')) > 1 AND COALESCE(primeiro_nome_pai_std_r,'') = left(COALESCE(primeiro_nome_pai_std_l,''), 1)))))
                    )
                """)
                
                kept_f2id = con.execute(f"""
                    SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_idade}
                """).fetchone()[0]
                dropped_f2id = kept_f1id - kept_f2id
                print(f"    F2-ID: {kept_f2id:,} kept, {dropped_f2id:,} dropped")
                
                # F3-ID: Sobrenomes (mesma lógica F3)
                if kept_f2id > 0:
                    con.execute(f"""
                        UPDATE {table_name}
                        SET decisao_refinada = 0,
                            decision_reason = CASE
                                WHEN sobrenome_faltantes_pessoa IS NOT NULL AND NOT {resc_pessoa} AND (
                                    (nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pessoa > 0 OR sobrenome_trocados_pessoa > 0))
                                    OR (nome_score >= {score_faixa_b_min} AND nome_score < {score_faixa_c_min} AND (sobrenome_faltantes_pessoa > {max_sob_falt_pessoa} OR sobrenome_trocados_pessoa > {max_sob_troc_pessoa}))
                                ) THEN 'F3-ID: sobrenome pessoa insuficiente'
                                WHEN sobrenome_faltantes_mae IS NOT NULL AND NOT {resc_mae} AND (
                                    (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_mae > 0 OR sobrenome_trocados_mae > 0))
                                    OR (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                                    OR (mae_score >= {score_faixa_b_min} AND mae_score < {score_faixa_c_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                                ) THEN 'F3-ID: sobrenome mae insuficiente'
                                WHEN sobrenome_faltantes_pai IS NOT NULL AND NOT {resc_pai} AND (
                                    (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pai > 0 OR sobrenome_trocados_pai > 0))
                                    OR (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                                    OR (pai_score >= {score_faixa_b_min} AND pai_score < {score_faixa_c_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                                ) THEN 'F3-ID: sobrenome pai insuficiente'
                                ELSE NULL
                            END
                        WHERE decisao_refinada = 1 {filtro_idade}
                        AND (
                            (sobrenome_faltantes_pessoa IS NOT NULL AND NOT {resc_pessoa} AND (
                                (nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pessoa > 0 OR sobrenome_trocados_pessoa > 0))
                                OR (nome_score >= {score_faixa_b_min} AND nome_score < {score_faixa_c_min} AND (sobrenome_faltantes_pessoa > {max_sob_falt_pessoa} OR sobrenome_trocados_pessoa > {max_sob_troc_pessoa}))))
                            OR (sobrenome_faltantes_mae IS NOT NULL AND NOT {resc_mae} AND (
                                (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_mae > 0 OR sobrenome_trocados_mae > 0))
                                OR (mae_score >= 50 AND mae_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))
                                OR (mae_score >= {score_faixa_b_min} AND mae_score < {score_faixa_c_min} AND (sobrenome_faltantes_mae > {max_sob_falt_pais} OR sobrenome_trocados_mae > {max_sob_troc_pais} OR (sobrenome_faltantes_mae > 0 AND sobrenome_trocados_mae > 0)))))
                            OR (sobrenome_faltantes_pai IS NOT NULL AND NOT {resc_pai} AND (
                                (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= 50 AND nome_score < {score_faixa_b_min} AND (sobrenome_faltantes_pai > 0 OR sobrenome_trocados_pai > 0))
                                OR (pai_score >= 50 AND pai_score < {score_faixa_b_min} AND nome_score >= {score_faixa_b_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))
                                OR (pai_score >= {score_faixa_b_min} AND pai_score < {score_faixa_c_min} AND (sobrenome_faltantes_pai > {max_sob_falt_pais} OR sobrenome_trocados_pai > {max_sob_troc_pais} OR (sobrenome_faltantes_pai > 0 AND sobrenome_trocados_pai > 0)))))
                        )
                    """)
                    
                    kept_f3id = con.execute(f"""
                        SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1 {filtro_idade}
                    """).fetchone()[0]
                    dropped_f3id = kept_f2id - kept_f3id
                    print(f"    F3-ID: {kept_f3id:,} kept, {dropped_f3id:,} dropped")
    
    # ================================================================
    # RESUMO FINAL
    # ================================================================
    total_aprovados = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 1").fetchone()[0]
    total_dropados = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE decisao_refinada = 0").fetchone()[0]
    
    # Limpar colunas de trabalho temporárias
    con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS nome_score")
    con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS mae_score")
    con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS pai_score")
    con.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS data_score")
    
    print(f"  ✓ Pipeline complete: {total_aprovados:,} approved, {total_dropados:,} dropped")


#Identifying true matches from refinement to set id_global 
class UnionFind:
    """
    Union-Find (Disjoint Set Union) data structure for clustering.
    Implements path compression and union by rank for efficiency.
    """
    def __init__(self):
        self.parent = {}
        self.rank = {}
    
    def find(self, x):
        """Find root of x with path compression"""
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            return x
        
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        
        return self.parent[x]
    
    def union(self, x, y):
        """Unite sets containing x and y using union by rank"""
        root_x = self.find(x)
        root_y = self.find(y)
        
        if root_x == root_y:
            return  # Already in same set
        
        # Union by rank
        if self.rank[root_x] < self.rank[root_y]:
            self.parent[root_x] = root_y
        elif self.rank[root_x] > self.rank[root_y]:
            self.parent[root_y] = root_x
        else:
            self.parent[root_y] = root_x
            self.rank[root_x] += 1
    
    def get_clusters(self):
        """
        Return dictionary mapping each element to its cluster root.
        Also consolidates all elements to point directly to root.
        """
        clusters = {}
        for element in list(self.parent.keys()):
            root = self.find(element)
            clusters[element] = root
        return clusters


# ========================================================================================
# GENERATE FINAL DATAFRAMES WITH ALL ORIGINAL VARIABLES AND IDENTIFYING INDIVIDUAL CLUSTERS
# ========================================================================================
def criar_bases_finais(user_config: UserConfig, con: duckdb.DuckDBPyConnection):
    """
    Cria bases finais com dados originais usando DuckDB (sem carregar na RAM):
    - final_matches*.parquet: apenas registros com match
    - final_todos*.parquet: todos os registros

    Utiliza UNION ALL BY NAME para concatenar bases originais e LEFT JOIN com pares,
    tudo dentro do DuckDB com spill to disk. Exporta em chunks sequenciais se necessário.
    """
    print("\n" + "=" * 60)
    print("CREATING FINAL DATABASES WITH ORIGINAL DATA (DuckDB mode)")
    print("=" * 60)

    results_folder = Path(user_config.results_folder)
    complete_folder = Path(user_config.complete_files_folder)

    # -------------------------------------------------------------------------
    # 1. Registrar view das bases originais concatenadas via DuckDB
    # -------------------------------------------------------------------------
    arquivos_parquet = list(complete_folder.glob("*.parquet"))

    if not arquivos_parquet:
        print("  ⚠ No parquet files found in complete_files_folder. Skipping.")
        return

    print(f"\nFound {len(arquivos_parquet)} original databases in {complete_folder}")

    # Bases-referência (SIM/CADUNICO) com flag desligado no UserConfig: seus
    # registros NÃO-PAREADOS não entram nas bases finais. Identificadas pelo
    # nome do arquivo, com a mesma regra de get_normalized_source.
    fontes_apenas_pares = set()
    if not user_config.SIM_BASE_FINAL:
        fontes_apenas_pares.add("SIM")
    if not user_config.CADUNIC_BASE_FINAL:
        fontes_apenas_pares.add("CADUNICO")
    arquivos_apenas_pares = [
        a.stem for a in arquivos_parquet
        if get_normalized_source(a.stem) in fontes_apenas_pares
    ]
    if arquivos_apenas_pares:
        print(f"  ℹ Bases apenas-pares (ids não-pareados serão descartados das bases finais): {arquivos_apenas_pares}")

   # Log contagem de cada base
    for arquivo in arquivos_parquet:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{arquivo}')").fetchone()[0]
        print(f"  {arquivo.stem}: {count:,} records")

    # Usar glob nativo do DuckDB com union_by_name (evita conflito de tipos e nomes internos)
    glob_path = str(complete_folder / "*.parquet").replace("\\", "/")
    
    # Ler todas as bases com union por nome de coluna + coluna de origem
    con.execute(f"""
        CREATE OR REPLACE VIEW bases_originais_raw AS
        SELECT * FROM read_parquet('{glob_path}', union_by_name=true, filename=true)
    """)
    
    # Obter colunas e cast tudo pra VARCHAR + criar fonte_base a partir do filename
    raw_cols = [row[0] for row in con.execute("DESCRIBE bases_originais_raw").fetchall()]
    
    cast_parts = []
    for col in raw_cols:
        if col == "filename":
            continue
        cast_parts.append(f'CAST("{col}" AS VARCHAR) AS "{col}"')
    
    # Adicionar fonte_base derivado do filename (nome do arquivo sem extensão)
    if "fonte_base" not in raw_cols:
        cast_parts.append("REGEXP_REPLACE(REGEXP_REPLACE(filename, '.*[/\\\\]', ''), '\\.parquet$', '') AS fonte_base")
    
    cast_sql = ", ".join(cast_parts)
    
    con.execute(f"""
        CREATE OR REPLACE VIEW bases_originais_view AS
        SELECT {cast_sql} FROM bases_originais_raw
    """)

    total_original = con.execute("SELECT COUNT(*) FROM bases_originais_view").fetchone()[0]
    num_cols_original = \
    con.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = 'bases_originais_view'").fetchone()[
        0]
    print(f"\nTotal original records: {total_original:,}")
    print(f"Total columns (union): {num_cols_original}")

    # -------------------------------------------------------------------------
    # 2. Carregar pares no DuckDB
    # -------------------------------------------------------------------------
    pares_c_match_path = results_folder / "pares_c_match.parquet"
    pares_todos_path = results_folder / "pares_todos.parquet"

    con.execute(f"""
        CREATE OR REPLACE VIEW pares_c_match_view AS
        SELECT * FROM read_parquet('{pares_c_match_path}')
    """)
    con.execute(f"""
        CREATE OR REPLACE VIEW pares_todos_view AS
        SELECT * FROM read_parquet('{pares_todos_path}')
    """)

    pares_c_match_count = con.execute("SELECT COUNT(*) FROM pares_c_match_view").fetchone()[0]
    pares_todos_count = con.execute("SELECT COUNT(*) FROM pares_todos_view").fetchone()[0]
    print(f"\nLoaded pares_c_match: {pares_c_match_count:,} records")
    print(f"Loaded pares_todos: {pares_todos_count:,} records")

    # -------------------------------------------------------------------------
    # 3. Função interna: des-triplicar, fazer join, exportar em chunks
    # -------------------------------------------------------------------------

    # Colunas de controle dos pares
    colunas_pares_base = ["unique_id", "cluster_id", "id_global", "fonte", "have_match", "pareado", "pareado_sim",
                          "SUB_PAR_SIM"]

    def exportar_base_final(view_pares: str, nome_saida: str, total_registros: int, is_todos: bool):
        """
        Gera final_matches ou final_todos em chunks via DuckDB.

        view_pares: nome da view dos pares (pares_c_match_view ou pares_todos_view)
        nome_saida: prefixo do arquivo (ex: "final_matches", "final_todos")
        total_registros: número total de registros esperados na saída
        is_todos: True para final_todos (bases LEFT JOIN pares), False para final_matches (pares LEFT JOIN bases)
        """
        print(f"\nCreating {nome_saida}...")

        # --- Des-triplicar: verificar se unique_id_original existe nos pares ---
        cols_pares = [row[0] for row in con.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{view_pares}'").fetchall()]

        colunas_select = [c for c in colunas_pares_base if c in cols_pares]
        colunas_select_sql = ", ".join([f'p."{c}"' for c in colunas_select])

        # Criar view des-triplicada dos pares
        if "unique_id_original" in cols_pares:
            # Ao colapsar as 3 variantes de volta ao original, manter a que REALMENTE
            # pareou (a que tem cluster_id / have_match), não a primeira por ordem
            # alfabética — senão o original herdaria um id_global de singleton vazio.
            if "cluster_id" in cols_pares:
                prioridade = '(p."cluster_id" IS NOT NULL) DESC, '
            elif "have_match" in cols_pares:
                prioridade = 'p."have_match" DESC, '
            else:
                prioridade = ''
            con.execute(f"""
                CREATE OR REPLACE VIEW pares_dedup_view AS
                SELECT {colunas_select_sql.replace('p."unique_id"',
                                                   'COALESCE(p."unique_id_original", p."unique_id") AS "unique_id"')}
                FROM {view_pares} p
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY COALESCE(p."unique_id_original", p."unique_id") 
                    ORDER BY {prioridade}p."unique_id"
                ) = 1
            """)
            antes = con.execute(f"SELECT COUNT(*) FROM {view_pares}").fetchone()[0]
            depois = con.execute("SELECT COUNT(*) FROM pares_dedup_view").fetchone()[0]
            if antes != depois:
                print(f"  Des-triplicated: {antes:,} → {depois:,} records")
        else:
            colunas_select_sql_direct = ", ".join([f'"{c}"' for c in colunas_select])
            con.execute(f"""
                CREATE OR REPLACE VIEW pares_dedup_view AS
                SELECT {colunas_select_sql_direct} FROM {view_pares}
            """)

        # --- Construir a query do join final ---
        # Projetar apenas as colunas de controle que REALMENTE existem na view
        # (espelha a lógica Polars de 21: só traz o que existe, sem nomear ausentes)
        ctrl_cols = [c for c in colunas_select if c not in ("unique_id", "fonte")]
        ctrl_sql = ", ".join([f'p."{c}"' for c in ctrl_cols])

        if is_todos:
            # Filtro bases-referência: descarta os registros NÃO-PAREADOS das
            # bases SIM/CADUNICO marcadas como False (identificadas pelo nome do
            # arquivo em fonte_base). pareado NULL = registros que nunca entraram
            # na dedup (descartados na limpeza) -> contam como não-pareados.
            # Os registros dessas bases que PAREARAM (pareado=1) permanecem.
            if arquivos_apenas_pares:
                _lista_ap = ", ".join([f"'{s}'" for s in arquivos_apenas_pares])
                filtro_apenas_pares = (
                    f'WHERE NOT (b.fonte_base IN ({_lista_ap}) '
                    f'AND COALESCE(p."pareado", 0) = 0)'
                )
            else:
                filtro_apenas_pares = ""
            # final_todos: bases_originais LEFT JOIN pares (todos os registros originais)
            join_sql = f"""
                SELECT b.*,
                       {ctrl_sql}
                FROM bases_originais_view b
                LEFT JOIN pares_dedup_view p ON b.unique_id = p.unique_id
                {filtro_apenas_pares}
            """
        else:
            # final_matches: pares LEFT JOIN bases (só quem pareou)
            join_sql = f"""
                SELECT {ctrl_sql},
                       b.*
                FROM pares_dedup_view p
                LEFT JOIN bases_originais_view b ON p.unique_id = b.unique_id
            """

        # Criar view do resultado final
        con.execute(f"""
            CREATE OR REPLACE VIEW resultado_join_view AS
            {join_sql}
        """)

        # --- Para final_todos: atribuir id_global aos registros sem par ---
        if is_todos:
            n_sem_id = con.execute("SELECT COUNT(*) FROM resultado_join_view WHERE id_global IS NULL").fetchone()[0]

            if n_sem_id > 0:
                print(f"  Found {n_sem_id:,} original records not in dedup pipeline. Assigning unique id_global...")

                # Obter max id_global existente
                max_existing = con.execute("""
                    SELECT COALESCE(MAX(CAST(SPLIT_PART(REPLACE(id_global, 'cluster_', ''), '_', 1) AS INTEGER)), 0)
                    FROM resultado_join_view
                    WHERE id_global IS NOT NULL
                """).fetchone()[0]

                next_id = max_existing + 1
                num_digits = len(str(next_id + n_sem_id))

                # Atribuir id_global sequencial aos nulos.
                # DuckDB nao aceita window function em UPDATE, entao recriamos a tabela
                # com ROW_NUMBER no SELECT. REPLACE mantem a posicao original da coluna.
                con.execute(f"""
                    CREATE OR REPLACE TABLE resultado_final_table AS
                    SELECT * REPLACE (
                        CASE
                            WHEN id_global IS NULL THEN 'cluster_' || LPAD(
                                CAST({next_id} + ROW_NUMBER() OVER (
                                    PARTITION BY (id_global IS NULL) ORDER BY unique_id
                                ) - 1 AS VARCHAR),
                                {num_digits}, '0'
                            )
                            ELSE id_global
                        END AS id_global
                    )
                    FROM resultado_join_view
                """)

                # Preencher colunas de controle
                con.execute("""
                    UPDATE resultado_final_table
                    SET have_match = 0, pareado = 0, pareado_sim = 0, SUB_PAR_SIM = 0
                    WHERE have_match IS NULL
                """)

                print(f"  Assigned {n_sem_id:,} unique id_global to records from original bases")

                # Trocar a view pela tabela materializada
                con.execute("DROP VIEW IF EXISTS resultado_join_view")
                source_name = "resultado_final_table"
            else:
                source_name = "resultado_join_view"
        else:
            source_name = "resultado_join_view"

        # --- Garantir have_match ---
        # Verificar se have_match existe no resultado
        result_cols = [row[0] for row in con.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{source_name}'
        """).fetchall()]

        if "have_match" not in result_cols:
            if source_name.endswith("_table"):
                # É tabela, pode fazer ALTER + UPDATE
                con.execute(f"ALTER TABLE {source_name} ADD COLUMN IF NOT EXISTS have_match INTEGER DEFAULT 0")
                if "cluster_id" in result_cols:
                    con.execute(f"""
                        UPDATE {source_name} 
                        SET have_match = CASE WHEN cluster_id IS NOT NULL THEN 1 ELSE 0 END
                    """)

        # --- Calcular chunks e exportar ---
        total_saida = con.execute(f"SELECT COUNT(*) FROM {source_name}").fetchone()[0]
        num_cols_saida = len(result_cols) if result_cols else \
        con.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{source_name}'").fetchone()[0]

        # Estimativa de memória: bytes médios por célula (strings curtas ~30 bytes em média)
        AVG_BYTES_PER_CELL = 30
        estimated_total_bytes = total_saida * num_cols_saida * AVG_BYTES_PER_CELL
        estimated_total_mb = estimated_total_bytes / (1024 * 1024)

        available_mb = get_available_memory_gb() * 1024
        limit_mb = available_mb * 0.75

        if estimated_total_mb > limit_mb and limit_mb > 0:
            n_chunks = max(1, math.ceil(estimated_total_mb / limit_mb))
        else:
            n_chunks = 1

        rows_per_chunk = math.ceil(total_saida / n_chunks)

        print(f"  Total records: {total_saida:,}")
        print(f"  Estimated size: {estimated_total_mb:,.0f} MB | RAM limit (75%): {limit_mb:,.0f} MB")
        print(f"  Chunks: {n_chunks}")

        arquivos_gerados = []
        for i in range(n_chunks):
            offset = i * rows_per_chunk

            if n_chunks == 1:
                arquivo_saida = results_folder / f"{nome_saida}.parquet"
            else:
                arquivo_saida = results_folder / f"{nome_saida}_{str(i + 1).zfill(2)}.parquet"

            con.execute(f"""
                COPY (
                    SELECT * FROM {source_name}
                    LIMIT {rows_per_chunk} OFFSET {offset}
                ) TO '{arquivo_saida}' (FORMAT PARQUET)
            """)

            chunk_count = min(rows_per_chunk, total_saida - offset)
            print(f"  ✓ Saved: {arquivo_saida.name} ({chunk_count:,} records)")
            arquivos_gerados.append(arquivo_saida)

        # Para final_todos: materializar a versão ENXUTA (só ids + flags) que
        # será salva como pares_todos.parquet. Materializa em TABELA para soltar
        # a dependência do arquivo pares_todos.parquet (que será sobrescrito
        # depois). fonte é derivada do nome do arquivo, igual get_normalized_source.
        if is_todos:
            con.execute(f"""
                CREATE OR REPLACE TABLE pares_todos_lean AS
                SELECT
                    unique_id,
                    id_global,
                    CASE
                        WHEN UPPER(fonte_base) LIKE '%SINASC%' THEN 'SINASC'
                        WHEN UPPER(fonte_base) LIKE '%CADUNICO%' THEN 'CADUNICO'
                        WHEN UPPER(fonte_base) LIKE '%SIM%' THEN 'SIM'
                        ELSE fonte_base
                    END AS fonte,
                    have_match,
                    pareado,
                    pareado_sim,
                    SUB_PAR_SIM
                FROM {source_name}
            """)

        # Para final_matches: materializar a versão ENXUTA e COLAPSADA (id original,
        # sem sufixo de variante) que será salva como pares_c_match.parquet. Sai da
        # pares_dedup_view, que já está des-triplicada (uma linha por registro original).
        if not is_todos:
            con.execute(f"""
                CREATE OR REPLACE TABLE pares_c_match_lean AS
                SELECT unique_id, id_global, fonte, SUB_PAR_SIM
                FROM pares_dedup_view
            """)

        # Limpar
        con.execute("DROP VIEW IF EXISTS pares_dedup_view")
        con.execute("DROP VIEW IF EXISTS resultado_join_view")
        con.execute("DROP TABLE IF EXISTS resultado_final_table")

        return total_saida, num_cols_saida, arquivos_gerados

    # -------------------------------------------------------------------------
    # 4. Gerar as duas bases finais
    # -------------------------------------------------------------------------
    matches_total, matches_cols, matches_files = exportar_base_final(
        "pares_c_match_view", "final_matches", pares_c_match_count, is_todos=False
    )

    todos_total, todos_cols, todos_files = exportar_base_final(
        "pares_todos_view", "final_todos", pares_todos_count, is_todos=True
    )

    # Limpar views
    con.execute("DROP VIEW IF EXISTS bases_originais_view")
    con.execute("DROP VIEW IF EXISTS pares_c_match_view")
    con.execute("DROP VIEW IF EXISTS pares_todos_view")

    # Sobrescrever pares_todos.parquet com a versão final ENXUTA e COMPLETA:
    # apenas ids + id_global + flags, cobrindo TODOS os registros das bases
    # originais (inclusive os que foram descartados antes da dedup), já com o
    # filtro de bases-referência aplicado. Só agora, depois de soltar a view
    # pares_todos_view que ainda apontava para o arquivo antigo.
    con.execute(f"COPY pares_c_match_lean TO '{pares_c_match_path}' (FORMAT PARQUET)")
    con.execute("DROP TABLE IF EXISTS pares_c_match_lean")
    pares_c_match_lean_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{pares_c_match_path}')"
    ).fetchone()[0]
    print(f"  ✓ Overwrote {pares_c_match_path.name} (índice colapsado de ids): {pares_c_match_lean_count:,} records")
    con.execute(f"COPY pares_todos_lean TO '{pares_todos_path}' (FORMAT PARQUET)")
    con.execute("DROP TABLE IF EXISTS pares_todos_lean")
    pares_todos_lean_count = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{pares_todos_path}')"
    ).fetchone()[0]
    print(f"  ✓ Overwrote {pares_todos_path.name} (índice enxuto de ids): {pares_todos_lean_count:,} records")

    print("\n" + "=" * 60)
    print("✓ FINAL DATABASES CREATED!")
    print("=" * 60)

    for f in matches_files:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        print(f"  {f.name}: {count:,} records with {matches_cols} columns")
    for f in todos_files:
        count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        print(f"  {f.name}: {count:,} records with {todos_cols} columns")

# ========================================================================================
# MAIN EXECUTION
# ========================================================================================

user_config = get_user_config()

print("### Convertendo arquivos Excel para CSV...\n\n")
converter_excel_csv(data_folder=user_config.data_folder)

Path(user_config.complete_files_folder).mkdir(parents=True, exist_ok=True)
Path(user_config.results_folder).mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=user_config.database_file)
threads_disponiveis = max(1, os.cpu_count() - 2)
con.execute(f"SET threads TO {threads_disponiveis}")
print(f"DuckDB configurado com {threads_disponiveis} threads (de {os.cpu_count()} disponíveis)")

memory_config = configure_duckdb_memory(con, memory_percentage=0.75)
print("\n" + "="*60)
print(memory_config)
print("="*60 + "\n")

# Start timer
start_time = time.time()

# Verificar se já existe base pré-processada
standardized_file = Path(user_config.results_folder) / "base_padronizada_pre_dedup.parquet"

if standardized_file.exists():
    print("="*60)
    print("CACHE ENCONTRADO - PULANDO PRÉ-PROCESSAMENTO")
    print("="*60)
    print(f"Arquivo encontrado: {standardized_file}")
    
    # Carregar contagem de registros para informação
    count_query = f"SELECT COUNT(*) FROM read_parquet('{standardized_file}')"
    total_records = con.execute(count_query).fetchone()[0]
    print(f"Registros pré-processados: {total_records:,}")
    print("Pulando Part 1 (leitura) e Part 2 (padronização)...")
    print("="*60 + "\n")
    
else:
    print("="*60)
    print("INICIANDO PRÉ-PROCESSAMENTO")
    print("="*60)
    print("Arquivo pré-processado não encontrado.")
    print("Executando Part 1 e Part 2...")
    print("="*60 + "\n")
    
    # PART 1: Read files
    file_metadata = part1_read_files(user_config, con)

    # PART 2: Standardize data (includes memory cleanup at the end)
    part2_standardize_data(user_config, con)

# Checkpoint: re-consolidação a partir de auditoria editada
results_folder = Path(user_config.results_folder)
audit_parquet = results_folder / "pares_auditoria.parquet"
pares_c_match_path = results_folder / "pares_c_match.parquet"
pares_todos_path = results_folder / "pares_todos.parquet"
final_matches_existem = list(results_folder.glob("final_matches*.parquet"))
final_todos_existem = list(results_folder.glob("final_todos*.parquet"))

bases_finais_completas = (
    pares_c_match_path.exists() and pares_todos_path.exists()
    and len(final_matches_existem) > 0 and len(final_todos_existem) > 0
)

if audit_parquet.exists() and not bases_finais_completas:
    print("="*60)
    print("RE-CONSOLIDACAO A PARTIR DE AUDITORIA EDITADA")
    print("="*60)
    print(f"Arquivo encontrado: {audit_parquet}")
    print("Bases finais ausentes — sincronizando decisoes e regenerando...")
    
    # 1. Ler parquet editado para DuckDB
    con.execute(f"""
        CREATE TEMP TABLE auditoria_editada AS 
        SELECT unique_id_l, unique_id_r, decisao_final 
        FROM read_parquet('{audit_parquet}')
    """)
    
    # 2. Verificar se pares_auditoria_acumulado existe no DuckDB
    tabela_existe = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_name = 'pares_auditoria_acumulado'
    """).fetchone()[0] > 0
    
    if tabela_existe:
        # Atualizar decisao_final no DuckDB com valores do parquet editado
        atualizado = con.execute("""
            UPDATE pares_auditoria_acumulado a
            SET decisao_final = e.decisao_final
            FROM auditoria_editada e
            WHERE a.unique_id_l = e.unique_id_l 
            AND a.unique_id_r = e.unique_id_r
        """).fetchone()
        print(f"  Decisoes sincronizadas com parquet editado")
    else:
        # Se o DuckDB não tem a tabela (raro), recriar a partir do parquet
        print("  Tabela pares_auditoria_acumulado não encontrada no DuckDB.")
        print("  Recriando a partir do parquet...")
        con.execute(f"""
            CREATE TABLE pares_auditoria_acumulado AS 
            SELECT * FROM read_parquet('{audit_parquet}')
        """)
    
    con.execute("DROP TABLE IF EXISTS auditoria_editada")

    # 3. Remover bases intermediárias para forçar re-geração
    for p in [pares_c_match_path, pares_todos_path]:
        if p.exists():
            p.unlink()
            print(f"  Removido: {p.name}")
    # Remover chunks de bases finais (podem ser múltiplos arquivos)
    for pattern in ["final_matches*.parquet", "final_todos*.parquet"]:
        for p in results_folder.glob(pattern):
            p.unlink()
            print(f"  Removido: {p.name}")
    
    # 4. Executar consolidação e bases finais
    consolidate_results(user_config, con)
    criar_bases_finais(user_config, con)
    
    print("="*60)
    print("RE-CONSOLIDACAO COMPLETA")
    print("="*60)

else:
    # Fluxo normal
    # PART 3: Deduplication
    part3_deduplicate(user_config, con)

    # CRIAR BASES FINAIS
    criar_bases_finais(user_config, con)

# Calculate execution time
elapsed_time = time.time() - start_time
hours = int(elapsed_time // 3600)
minutes = int((elapsed_time % 3600) // 60)
seconds = int(elapsed_time % 60)

# Format time string
if hours > 0:
    time_str = f"{hours}h {minutes}m {seconds}s"
elif minutes > 0:
    time_str = f"{minutes}m {seconds}s"
else:
    time_str = f"{seconds}s"

print("\n" + "="*60)
print("✓ ALL PROCESSING COMPLETE!")
print("="*60)
print(f"Results saved in: {user_config.results_folder}/")
print(f"Total execution time: {time_str}")

con.close()