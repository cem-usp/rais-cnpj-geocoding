# ------------------------- #
# Calcular matriz de tempo
# de viagem para municipios
# ------------------------- #

# Necessarias etapas de instalacao do ambiente:
# 1. Instalar o Java Development Kit (JDK)
#    - Baixar do site oficial: https://www.oracle.com/java/technologies/javase-downloads.html
#    - Escolher a versao adequada (por exemplo, JDK 17)
#    - Seguir instalacao padrao do sistema operacional
#    - Configurar a variavel de ambiente JAVA_HOME apontando para a pasta de instalacao do JDK
#    - Adicionar JAVA_HOME/bin ao PATH do sistema

# 2. Instalar o Osmosis (para processamento de OSM)
#    - Baixar do site oficial: https://wiki.openstreetmap.org/wiki/Osmosis
#    - Descompactar em uma pasta local
#    - Adicionar o diretorio bin do Osmosis ao PATH do sistema (opcional)
#    - Testar executando 'osmosis --help' no terminal para verificar se funciona


# Bibliotecas ----
library(arrow) # arquivos .parquet
library(sf) # oreacoes de geometria
library(tictoc) # tempo de processamento
library(r5r) # Calculo de tempos de viagem entre pontos
library(dplyr)

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

# Calcular matriz de tempo de viagem -----------------------------------

# Configurações do R5R -------------------------------------------------
options(java.parameters = c("-Xmx2G", "-XX:ActiveProcessorCount=4"))
Sys.setenv(JAVA_HOME = "C:/Program Files/Java/jdk-21")

hex <- 9
mode <- c("WALK")
max_walk_dist <- 3000
max_trip_duration <- 60
departure_datetime <- as.POSIXct("13-05-2019 14:00:00",
                                 format = "%d-%m-%Y %H:%M:%S")

class <- c("g0", "g1_g2", "g3", "g4")


for (mun in municipios) {
  
  message("Processando municipio: ", mun)
  
  # Caminhos ------------------------------------------------------------
  dir_mun <- file.path(dir_parcial, mun)
  
  osmosis_path <- file.path(dir_inputs, "OSM/osmosis-0.49.2/bin/osmosis.bat")
  br_pbf <- file.path(dir_inputs, "OSM/brazil-latest-filtered.osm.pbf")
  mun_pbf <- file.path(dir_mun, "redeviaria.osm.pbf")
  
  caminho_hex_urbanizado <- file.path(dir_mun, "hex/hex_urbanizado.parquet")
  
  # Extrair bounding box dos hexagonos ---------------------------------
  mun_hex <- sfarrow::st_read_parquet(caminho_hex_urbanizado)
  mun_bbox <- st_bbox(mun_hex)
  
  # Comando Osmosis -----------------------------------------------------
  osmosis_cmd <- sprintf(
    "%s --read-pbf %s --bounding-box left=%s bottom=%s right=%s top=%s --write-pbf %s",
    osmosis_path, br_pbf,
    mun_bbox["xmin"], mun_bbox["ymin"],
    mun_bbox["xmax"], mun_bbox["ymax"],
    mun_pbf
  )
  
  # Executar Osmosis ----------------------------------------------------
  tictoc::tic(msg = paste("Extraindo malha OSM para", mun))
  shell(osmosis_cmd, translate = TRUE)
  tictoc::toc()
  
  # Inicializar R5 ------------------------------------------------------
  caminho_core <- file.path(dir_parcial, mun)
  r5r_core <- r5r::setup_r5(data_path = caminho_core, verbose = FALSE)
  
  # Preparar centroides dos hexagonos ----------------------------------
  centroides <- mun_hex %>%
    st_centroid() %>%
    mutate(
      id = h3_address,
      lat = st_coordinates(.)[, 2],
      lon = st_coordinates(.)[, 1]
    ) %>%
    st_drop_geometry() %>%
    arrange(id)

  # Loop sobre class ----------------------------------------------------
  for (class_atual in class) {
    message("Classificação: ", class_atual)
    
    caminho_rais <- file.path(dir_mun, "rais", paste0(class_atual, "_geocod.parquet"))
    if (!file.exists(caminho_rais)) {
      message("[Aviso] Arquivo não encontrado: ", caminho_rais)
      next
    }
    
    # Preparar RAIS geocodificado
    rais <- read_parquet(caminho_rais) 
    
    # Contar NAs antes de remover
    n_na <- sum(is.na(rais$lat) | is.na(rais$lon))
    message("Removendo ", n_na, " registros sem coordenadas (lat/lon NA)")
    
    # Dropar NAs e ordenar
    rais <- rais %>%
      tidyr::drop_na(lat, lon)
    
    
    rais <- rais %>%
      st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
      st_transform(st_crs(mun_hex)) %>%
      mutate(
        id = cnpj,
        lat = st_coordinates(.)[, 2],
        lon = st_coordinates(.)[, 1]
      ) %>%
      st_drop_geometry() %>%
      arrange(id)
    
    # Calcular matriz de tempo ------------------------------------------
    ttm <- r5r::travel_time_matrix(
      r5r_core = r5r_core,
      origins = centroides,
      destinations = rais,
      mode = mode,
      departure_datetime = departure_datetime,
      max_trip_duration = max_trip_duration,
      verbose = FALSE
    )
    
    # Salvar resultado --------------------------------------------------
    out_dir <- file.path(dir_mun, "rais", "tempos")
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    
    caminho_saida <- file.path(out_dir, paste0("ttm_", class_atual, ".rds"))
    saveRDS(ttm, file = caminho_saida)
    message("Arquivo salvo: ", caminho_saida)
  }
  
  # Limpar memória ------------------------------------------------------
  rm(r5r_core)
  gc()
  
  message("Municipio ", mun, " concluido.")
}