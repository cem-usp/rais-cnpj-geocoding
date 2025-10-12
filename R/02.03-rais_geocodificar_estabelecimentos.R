# --------------------------------------------- #
# Geocodifica enderecos do cnpj referentes a rais
# tendo como resultado um arquivo de join entre 
# rais e enderecos geocodificados
# --------------------------------------------- #

# Bibliotecas ---

library(dplyr)       # Manipulação de dados  
library(stringi)     # Tratamento de strings  
library(readr)       # Leitura e escrita de arquivos  
library(arrow)       # Arquivos parquet e feather  
library(geocodebr)   # Geocodificação de endereços no Brasil  
library(stringr)     # Manipulação de strings  
library(geobr)       # Processar geometrias dos municipios
library(sf)          # Processamento de geometrias

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

# definir tipos de estabelecimento
class <- c("g0", 
           "g1_g2", 
           "g3", 
           "g4"
           )

# Loop sobre arquivos de CNPJs -----------------------------------------
for (class_atual in class) {
  
  # Extrair tipo do nome do arquivo
  message(">> Tipo identificado: ", class_atual)
  
  # Ler arquivo de endereços
  cnpj_enderecos <- arrow::read_parquet(file.path(dir_parcial, "cnpj_enderecos_brasil", paste0("cnpj_brasil_", class_atual, ".parquet")))
  
  # Ajustar CEP
  cnpj_enderecos$cep <- sprintf("%08d", as.numeric(trimws(cnpj_enderecos$cep)))
  
  # Criar endereço completo
  cnpj_enderecos$endereco <- paste(
    cnpj_enderecos$tipo_logradouro, 
    cnpj_enderecos$logradouro
  )
  
  # Ajustar número
  cnpj_enderecos$numero <- iconv(cnpj_enderecos$numero, from = "latin1", to = "ASCII//TRANSLIT")
  cnpj_enderecos$numero <- gsub("\\D", "", cnpj_enderecos$numero)
  cnpj_enderecos$numero <- suppressWarnings(as.character(as.numeric(trimws(cnpj_enderecos$numero))))
  
  # Municipios UTF-8
  cnpj_enderecos$municipio_utf8 <- toupper(
    stri_trans_general(cnpj_enderecos$no_municipio, "Latin-ASCII")
  )
  
  # Selecionar colunas
  cnpj_enderecos <- cnpj_enderecos %>% select(
    cnpj, uf, municipio_utf8, endereco, numero, cep, bairro
  )
  
  # Converter para UTF-8
  cnpj_enderecos[] <- lapply(cnpj_enderecos, function(col) {
    if (is.character(col)) {
      Encoding(col) <- "UTF-8"
      return(iconv(col, from = "", to = "UTF-8", sub = ""))
    } else {
      return(col)
    }
  })
  
  # Definir campos de geocodificação
  campos <- geocodebr::definir_campos(
    estado     = "uf",
    municipio  = "municipio_utf8",
    logradouro = "endereco",
    numero     = "numero",
    cep        = "cep",
    localidade = "bairro"
  )
  
  # Geocodificar endereços
  geocodificado <- geocodebr::geocode(
    enderecos        = cnpj_enderecos,
    campos_endereco  = campos,
    resultado_sf     = FALSE,
    verboso          = TRUE,
    cache            = TRUE,
    n_cores          = 1,
    resolver_empates = TRUE
  )
  
  # Selecionar variáveis finais
  geocodificado <- geocodificado %>% select(cnpj, lat, lon)
  geocodificado <- geocodificado[!is.na(geocodificado$lat) & !is.na(geocodificado$lon), ]
  
  # Converter para sf
  geocodificado_sf <- st_as_sf(
    geocodificado,
    coords = c("lon", "lat"),
    crs = 4326
  )
  
  # Loop por municipio --------------------------------------------------
  for (mun in municipios) {
    message("  -> Municipio: ", mun)
    
    # Geometria do municipio
    geom_mun <- read_municipality(code_muni = mun, year = 2022) %>%
      st_transform(crs = 4326)
    
    # Filtrar endereços dentro do municipio
    geocodificado_mun <- geocodificado_sf %>%
      st_join(geom_mun, join = st_within, left = FALSE)
    
    # Converter de volta para data frame
    geocodificado_mun <- geocodificado_mun %>%
      mutate(
        lon = st_coordinates(.)[,1],
        lat = st_coordinates(.)[,2]
      ) %>%
      st_drop_geometry() %>%
      select(cnpj, lat, lon)
    
    # Caminho do arquivo RAIS referente ao tipo atual
    rais_path <- file.path(dir_parcial, mun, "rais", paste0("rais_", class_atual, ".parquet"))
    if (!file.exists(rais_path)) next
    
    # Ler RAIS
    rais <- arrow::read_parquet(rais_path)
    
    # Join com coordenadas
    rais_geo <- rais %>%
      left_join(geocodificado_mun, by = "cnpj")
    
    # Salvar parquet (com nome do tipo)
    caminho_saida <- file.path(dir_parcial, mun, "rais", paste0(class_atual, "_geocod.parquet"))
    arrow::write_parquet(rais_geo, caminho_saida)
  }
}


