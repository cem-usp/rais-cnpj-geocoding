# --------------------------------------------------------------------------- #
# Para dar proceguinto ao codigo, baixar todos os arquivos da 
# Receita Federal do Brasil, manter .zips
# Mes de referencia: 2025-01 (janeiro de 2025)
# https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/iC=N;O=D
# Armazenar arquivos em CNPJ_2024 dentro de diretorio "inputs"
# --------------------------------------------------------------------------- #

# Bibliotecas ---

library(dplyr)       # Manipulacao de data frames 
library(stringr)     # Funcoes de manipulacao de strings 
library(data.table)  # Leitura rapida de CSVs grandes 
library(arrow)       # Leitura e escrita de arquivos parquet 
library(utils)       # Funcoes basicas do R, incluindo unzip()

# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # Deixar arquivos de input aqui
dir_parcial <- "2-parcial" # Arquivos com algum processamento


# Listar municipios -----------------------------------------------------
municipios <- c(
  3550308,   # Sao Paulo
  2507507,   # Joao Pessoa
  3106200,   # Belo Horizonte
  4314902,   # Porto Alegre
  1721000,   # Palmas
  5300108,   # Brasilia
  5208707    # Goiania
)

# Defnir diretorios de cnpj
cnpj_dir <- "1-inputs/CNPJ_2024"
estabelecimentos_dir <- "1-inputs/CNPJ_2024/1-ESTABELECIMENTOS"
empresas_dir <- "1-inputs/CNPJ_2024/2-EMPRESAS"
socios_dir <- "1-inputs/CNPJ_2024/3-SOCIOS"
demais_arquivos_dir <- "1-inputs/CNPJ_2024/4-DEMAIS_ARQUIVOS"

# Descompactar arquivos de CNPJ e organiza-los em diretorios ------------
# PULAR ETAPA SE ARQUIVOS JA ESTIVEREM DESCOMPACTADOS

# Definir subdiretorios
subdiretorios <- c(
  "1-ESTABELECIMENTOS",
  "2-EMPRESAS",
  "3-SOCIOS",
  "4-DEMAIS_ARQUIVOS"
)

# Criar subdiretorios no diretorio caso nao existam
for (subdir in subdiretorios) {
  caminho <- file.path(cnpj_dir, subdir)
  
  # Verificar se o diretorio existe
  if (!file.exists(caminho)) {
    dir.create(caminho, recursive = TRUE)
    cat("Diretorio criado:", caminho, "\n")
  } else {
    cat("Diretorio ja existe:", caminho, "\n")
  }
}

# Definir diretorios de destino para extracao
diretorios_destino <- list(
  "Estabelecimentos" = file.path(cnpj_dir, "1-ESTABELECIMENTOS"),
  "Empresas" = file.path(cnpj_dir, "2-EMPRESAS"),
  "Socios" = file.path(cnpj_dir, "3-SOCIOS"),
  "Demais_Arquivos" = file.path(cnpj_dir, "4-DEMAIS_ARQUIVOS")
)


# Definir funcao para descompactar os arquivos
unzip_arquivos <- function() {
  arquivos_zip <- list.files(cnpj_dir, 
                             pattern = "\\.zip$", 
                             full.names = TRUE, 
                             recursive = TRUE)
  
  for (zip_file_path in arquivos_zip) {
    # Define o diretorio de destino baseado no nome do arquivo zip
    file_name <- basename(zip_file_path)
    if (grepl("^Estabelecimentos", file_name)) {
      destino <- diretorios_destino$Estabelecimentos
    } else if (grepl("^Empresas", file_name)) {
      destino <- diretorios_destino$Empresas
    } else if (grepl("^Socios", file_name)) {
      destino <- diretorios_destino$Socios
    } else {
      destino <- diretorios_destino$Demais_Arquivos
    }
    
    # Cria o diretorio de destino se nao existir
    if (!dir.exists(destino)) {
      dir.create(destino, recursive = TRUE)
    }
    
    # Extracao do conteudo do arquivo .zip
    unzip(zip_file_path, exdir = destino)
    cat("Arquivos de", file_name, "extraidos para", destino, "\n")
  }
}

# Descompactar e organizar arquivos .zip
unzip_arquivos()

# Processar arquivos de cnpj ----------------------------------------------
# PULAR ETAPA SE JA ESTIVER CRIADO O ARQUIVO: 
# 2-parcial/cnpj_enderecos_brasil/cnpj_enderecos_brasil.parquet

# Carregar CNPJs de referencia de TODOS os tipos de RAIS ----------------

lista_cnpjs <- list()

for (mun in municipios) {
  
  rais_dir <- file.path(dir_parcial, mun, "rais")
  arquivos_rais <- list.files(rais_dir, pattern = "^rais_.*\\.parquet$", full.names = TRUE)

  for (arq in arquivos_rais) {
    tipo <- gsub("rais_|\\.parquet", "", basename(arq))  # g0, g1_g2, g3, g4
    
    rais_cnpj <- read_parquet(arq) %>%
      dplyr::select(cnpj) %>%
      mutate(
        cnpj = as.character(cnpj),
        municipio = mun
      ) %>%
      distinct()
    
    lista_cnpjs[[tipo]] <- bind_rows(lista_cnpjs[[tipo]], rais_cnpj)
    print(paste("Arquivo da RAIS lido:", arq))
  }
}

# Carregar referencia de municipios -------------------------------------
municipios_ref <- fread(
  file.path(demais_arquivos_dir, "F.K03200$Z.D50111.MUNICCSV"),
  sep = ";", encoding = "Latin-1",
  header = FALSE,
  col.names = c("MUNICIPIO_TOM", "no_municipio")
) %>%
  mutate(MUNICIPIO_TOM = as.character(MUNICIPIO_TOM))


# ------------------------------------------------
# Processar estabelecimentos por tipo de RAIS
# ------------------------------------------------

saida_dir <- file.path(dir_parcial, "cnpj_enderecos_brasil")
dir.create(saida_dir, recursive = TRUE, showWarnings = FALSE)

arquivos_estab <- list.files(estabelecimentos_dir, full.names = TRUE)
total_arquivos <- length(arquivos_estab)

for (tipo_rais in names(lista_cnpjs)) {
  
  print(paste("Processando RAIS:", tipo_rais))
  cnpjs_rais <- lista_cnpjs[[tipo_rais]]
  lista_estab <- list()
  
  for (i in seq_along(arquivos_estab)) {
    caminho_arquivo <- arquivos_estab[i]
    print(paste("Arquivo 'estabelecimentos'", i, "de", total_arquivos))
    
    tryCatch({
      estab <- fread(caminho_arquivo, sep = ";", encoding = "Latin-1", header = FALSE, col.names = c(
        "CNPJ_BASICO", "CNPJ_ORDEM", "CNPJ_DV", "IDENTIFICADOR_MATRIZ_FILIAL",
        "NOME_FANTASIA", "SITUACAO_CADASTRAL", "DATA_SITUACAO_CADASTRAL",
        "MOTIVO_SITUACAO_CADASTRAL", "NOME_CIDADE_EXTERIOR", "PAIS",
        "DATA_INICIO_ATIVIDADE", "CNAE_FISCAL_PRINCIPAL", "CNAE_FISCAL_SECUNDARIO",
        "tipo_logradouro", "logradouro", "numero", "complemento", "bairro",
        "cep", "uf", "MUNICIPIO_TOM", "DDD_1", "TELEFONE_1", "DDD_2", "TELEFONE_2",
        "DDD_FAX", "FAX", "CORREIO_ELETRONICO", "SITUACAO_ESPECIAL", "DATA_SITUACAO_ESPECIAL"
      ))
      
      # Ajustar CNPJs e CEP
      estab <- estab %>%
        mutate(
          CNPJ_BASICO = str_pad(CNPJ_BASICO, 8, pad = "0"),
          CNPJ_ORDEM  = str_pad(CNPJ_ORDEM, 4, pad = "0"),
          CNPJ_DV     = str_pad(CNPJ_DV, 2, pad = "0"),
          cnpj        = paste0(CNPJ_BASICO, CNPJ_ORDEM, CNPJ_DV),
          cep         = str_pad(as.character(cep), width = 8, side = "left", pad = "0"),
          MUNICIPIO_TOM = as.character(MUNICIPIO_TOM)
        ) %>%
        semi_join(cnpjs_rais, by = "cnpj") %>%   # pegar só CNPJs desta RAIS
        left_join(municipios_ref, by = "MUNICIPIO_TOM") %>%
        dplyr::select(cnpj, uf, no_municipio, tipo_logradouro, logradouro, numero, complemento, bairro, cep)
      
      lista_estab[[length(lista_estab) + 1]] <- estab
    }, error = function(e){
      message("Falha ao processar: ", caminho_arquivo, " - ", e$message)
    })
  }
  
  # Unir e salvar parquet por RAIS
  estab_total <- bind_rows(lista_estab)
  write_parquet(estab_total, file.path(saida_dir, paste0("cnpj_brasil_", tipo_rais, ".parquet")))
  print(paste("Arquivo salvo:", paste0("cnpj_brasil_", tipo_rais, ".parquet")))
}
