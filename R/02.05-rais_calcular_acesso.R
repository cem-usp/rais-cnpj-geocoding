# --------------------------------- #
# Produzir geometrias hexagonais para
# pontos de acesso a estabelecimentos
# --------------------------------- #

# Bibliotecas ----
library(arrow)     # leitura de arquivos .parquet
library(sf)        # operações de geometria
library(dplyr)     # manipulação de dados
library(sfarrow)   # leitura de parquet com geometria

# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # arquivos de input
dir_parcial <- "2-parcial" # arquivos intermediarios

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

# Listar categorias de estabelecimentos
categorias <- c("g0", "g1_g2", "g3", "g4")

# Calcular acesso -------------------------------------------------------
for (mun in municipios) {
  
  cat("Processando município:", mun, "\n")
  
  # Criar pasta 'acesso'
  dir_acesso <- file.path(dir_parcial, mun, "rais/acesso")
  if (!dir.exists(dir_acesso)) dir.create(dir_acesso, recursive = TRUE)
  
  # Ler hexagonos para municipio ---
  caminho_hex <- file.path(dir_parcial, mun, "hex/hex_urbanizado.parquet")
  hex <- sfarrow::st_read_parquet(caminho_hex)
  
  # Loop nas categorias
  for (tipo in categorias) {
    
    cat(" -> Categoria:", tipo, "\n")
    
    # --------------------------------------------------------------- #
    caminho_ttm <- file.path(dir_parcial, mun, "rais/tempos", paste0("ttm_", tipo, ".rds"))
    # Ler tempos de viagem
    ttm <- readRDS(caminho_ttm)
    
    # --------------------------------------------------------------- #
    # Numero de estabelecimentos ate 15 minutos
    n_acesso_15 <- ttm %>%
      mutate(tempo_15 = ifelse(travel_time_p50 <= 15, 1, 0)) %>%
      group_by(from_id) %>%
      summarise(est_15 = sum(tempo_15, na.rm = TRUE), .groups = "drop")
    
    # Numero de estabelecimentos por 1000 habitantes
    n_acesso_15 <- hex %>%
      left_join(n_acesso_15, by = c("h3_address" = "from_id")) %>%
      mutate(est_pop_15 = est_15 / pop_bas_mor_tot_pes * 1000) %>%
      rename(from_id = h3_address) %>%
      dplyr::select(from_id, est_pop_15)
    
    n_acesso_15$est_pop_15[is.infinite(n_acesso_15$est_pop_15)] <- NA
    
    # Salvar resultado
    write_sf(
      n_acesso_15,
      file.path(dir_acesso, paste0("n_estab_15_", tipo, ".gpkg"))
    )
    
    # # Tempo minimo médio dos 3 estabelecimentos mais proximos
    #  tempo_minimo_medio_3 <- ttm %>%
    #   group_by(from_id) %>%
    #   summarise(
    #     tempo_min_medio_3 = mean(head(sort(travel_time_p50), 3), na.rm = TRUE),
    #     .groups = "drop"
    #   )
    # 
    # # Juntar com hexagonos
    # mun_hex_med_3 <- hex %>%
    #   left_join(tempo_minimo_medio_3, by = c("h3_address" = "from_id")) #%>%
    #   #st_transform(31983)
    # 
    # # Salvar resultado
    # write_sf(
    #   mun_hex_med_3,
    #   file.path(dir_acesso, "tempo_minmed_3_estab.gpkg")
    # )
    
    cat("Concluído município:", mun, "\n")
  }
}
