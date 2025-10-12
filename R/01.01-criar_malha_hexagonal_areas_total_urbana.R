# ------------------------------ #
# Cria malhar hexagonais para 
# os municipios a serem analisados
# ------------------------------ #

# Aqui teria 2 etapas que estão faltando no script:
# 1 - trabalhar a area urbanizada do IBGE
# 2 - gerar hexagonos da area urbanizada

# Bibliotecas ----
library(geobr) # geometrias municipais
library(sf) # espaciais
library(tidyverse) # gerais
library(h3jsr) # get grid
library(arrow) # arquivos .parquet


# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # Deixar arquivos de input aqui
dir_parcial <- "2-parcial" # Arquivos com algum processamento

# Criar diretorio de arquivos parciais
dir.create(dir_parcial, showWarnings = FALSE, recursive = TRUE)

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

# Ler hexagonos urbanizados do Brasil com dados do censo ----------------
hexurb <- read_delim(
  file.path(dir_inputs, "censo/censo2022_hex.csv"
  ), delim = ","
) %>%
  # Filtrar fora linhas com somente 0 em todas as variaveis esceto h3_address
  filter(!if_all(-h3_address, ~ .x == 0))

# Criar malha hexagonal e separar entre urb e nao-urb -------------------
for (cod in municipios) {
  
  # Listar geometria do município
  municipio_geom <- read_municipality(code_muni = cod, year = 2020)
  
  # Converter poligono para células H3
  hex <- h3jsr::polygon_to_cells(geometry = municipio_geom$geom, res = 9)
  rm(municipio_geom)
  
  # Converter celulas H3 de volta para polígonos
  hexgrid <- h3jsr::cell_to_polygon(input = hex, simple = FALSE)
  
  print(paste("Malha hexagonal criada para", cod))
  
  # Separar hexagonos urbanizados
  hex_urb_mun <- hexgrid %>%
    left_join(hexurb, by = "h3_address") %>%
    filter(if_all(-h3_address, ~ !is.na(.x))) %>%
    mutate(across(where(is.numeric) & !any_of("h3_address"), abs))
  
  
  print("Filtragem realizada")
  
  # Criar diretorio
  dir_hex <- file.path(dir_parcial, as.character(cod), "hex")
  dir.create(dir_hex, showWarnings = FALSE, recursive = TRUE)
  
  # Salvar arquivos
  sfarrow::st_write_parquet(hexgrid, file.path(dir_hex, "hex.parquet"))    # Total
  sfarrow::st_write_parquet(hex_urb_mun, file.path(dir_hex, "hex_urbanizado.parquet")) # Urbanizado
}
