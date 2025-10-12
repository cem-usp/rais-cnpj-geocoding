# ----------------------- #
# Criar arquivo .tiff da 
# area de cada municipio 
# ----------------------- #

# Bibliotecas ----
library(arrow) # arquivos .parquet
library(raster) # escrever .tiff
library(elevatr) # gerar .tiff
library(sfarrow) # ler .parquet com geometria

# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # Deixar arquivos de input aqui
dir_parcial <- "2-parcial" # Arquivos com algum processamento

# Convigurar tempo de timeout
options(timeout = 1200)

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

# Criar arquivo .tiff para a AREA URBANIZADA de cada municipio ----------
for (cod in municipios) {
  
  # Definir caminho do arquivo .tiff
  elevation_path <- file.path(dir_parcial, cod, "elevation.tiff")
  
  # Ler hexgrid do município
  hexgrid <- st_read_parquet(file.path(dir_parcial, 
                                       cod, 
                                       "hex/hex_urbanizado.parquet"))
  
  # Criar raster de elevação (zoom z=13)
  elev_raster <- get_elev_raster(hexgrid, z = 13, override_size_check = TRUE)
  
  # Salvar .tiff
  writeRaster(elev_raster, elevation_path, overwrite = TRUE)
  
  print(paste("Arquivo .tiff criado para município", cod))
}
