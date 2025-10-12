# ---------------------------------------------- #
# Processar dados de raster para cobertura vegetal
# e exportar GeoPackage por municipio
# ---------------------------------------------- #

# Bibliotecas ---
library(terra)   # manipulacao de raster
library(geobr)   # geometria de municipios
library(sf)      # exportacao para GeoPackage
library(raster)

# Definir diretorios
setwd("D:/CEM_acessoSAN/")
dir_inputs <- "1-inputs" 
dir_parcial <- "2-parcial"

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

# Ler raster MapBiomas ---------------------------------------------------
caminho_raster <- file.path(dir_inputs, 
                            "mapbiomas_10m_collection2_integration_v1-classification_2023.tif")
r <- rast(caminho_raster)

# Processar raster por municipio --------------------------------------------
# Classificar os niveis
classes_labels <- list(
  "cobertura_area_verde"    = c(3,4,5,6,49),
  "cobertura_veg_herb_arb" = c(10,11,12,29,32,50),
  "cobertura_agropecuaria" = c(9,14,15,18,19,20,21,35,36,39,40,41,46,47,48,62),
  "cobertura_area_nao_vegetada" = c(22,23,24,25,30,75),
  "cobertura_agua"          = c(26,31,33,27) # agregar agua e nao observado
)

for(codigo in municipios){
  cat("Processando município:", codigo, "\n")
  
  # Geometria do municipio
  mun <- geobr::read_municipality(code_muni = codigo, year = 2020, simplified = FALSE)
  mun <- vect(mun)
  mun <- project(mun, crs(r))
  
  # Diretorio de saida
  dir_cobertura <- file.path(dir_parcial, codigo, "cobertura")
  if(!dir.exists(dir_cobertura)) dir.create(dir_cobertura, recursive=TRUE)
  
  # Recortar e mascarar raster
  r_mun <- crop(r, mun)
  r_mun <- mask(r_mun, mun)
  
  # Tornar inteiro
  r_mun <- round(r_mun)
  
  # Para cada classe
  for(classe_nome in names(classes_labels)){
    
    valores_classe <- classes_labels[[classe_nome]]
    
    # Raster binario
    r_bin <- ifel(r_mun %in% valores_classe, 1, NA)
    
    # Verificar se tem pixels
    if(any(!is.na(values(r_bin)))){
      
      # Converter para poligono
      polig_classe <- as.polygons(r_bin, dissolve=TRUE)
      polig_classe$classe <- classe_nome
      
      # Salvar
      arquivo_gpkg <- file.path(dir_cobertura, paste0(classe_nome, ".gpkg"))
      terra::writeVector(polig_classe, filename=arquivo_gpkg, filetype="GPKG", overwrite=TRUE)
      cat("Classe", classe_nome, "salva em:", arquivo_gpkg, "\n")
    } else {
      cat("Município", codigo, "- classe", classe_nome, "não possui pixels, pulando.\n")
    }
  }
}

