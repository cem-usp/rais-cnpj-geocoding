# ------------------------ #
# Criar arquivo de malha 
# viaria para cada municipio 
# ------------------------ #

# Bibliotecas ----
library(arrow) # arquivos .parquet
library(osmdata) # malha viaria
library(sf) # oreacoes de geometria
library(tictoc) # tempo de processamento



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

# Criar malha de transporte para a AREA URBANIZADA de cada municipio ----
# https://download.geofabrik.de/south-america/brazil.html
# -> Baixar brasil inteiro <-

for (cod in municipios) {
  
  print(paste("Processando", cod))
  
  # Definir diretório de saída
  dir_mun <- file.path(dir_parcial, cod)
  dir.create(dir_mun, showWarnings = FALSE, recursive = TRUE)
  
  # Bounding box do município
  mun_hex <- st_read_parquet(file.path(dir_mun, "hex/hex_urbanizado.parquet"))
  mun_bbox <- st_bbox(mun_hex)
  
  # Caminho Osmosis
  osmosis_path <- file.path(dir_inputs, "OSM/osmosis-0.49.2/bin/osmosis.bat")
  br_pbf <- file.path(dir_inputs, "OSM/brazil-latest-filtered.osm.pbf")
  mun_pbf <- file.path(dir_mun, "redeviaria.osm.pbf")
  
  # Comando Osmosis
  osmosis_cmd <- sprintf("%s --read-pbf %s --bounding-box left=%s bottom=%s right=%s top=%s --write-pbf %s",
                         osmosis_path, br_pbf, 
                         mun_bbox["xmin"], mun_bbox["ymin"], 
                         mun_bbox["xmax"], mun_bbox["ymax"],
                         mun_pbf)
  
  # Executa Osmosis
  tictoc::tic(msg = paste("Extraindo malha OSM para", cod))
  shell(osmosis_cmd, translate = TRUE)
  tictoc::toc()
  
}
