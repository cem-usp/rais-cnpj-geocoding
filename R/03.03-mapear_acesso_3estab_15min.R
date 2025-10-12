# ----------------------------------------- #
# Produzir mapa de tempo minimo medio de 
# acesso a estabelecimentos em ate 15 minutos
# ----------------------------------------- #

# Bibliotecas ----
library(arrow)     # leitura de arquivos .parquet
library(ggplot2)   # Fazer mapas
library(sf)        # Trabalhar com geometrias
library(dplyr)     # operacoes gerais
library(geobr)     # Ler geometrias de municipios
library(gridExtra)
library(grid)
library(ggnewscale)
library(cowplot)
library(ggspatial)

# Definir diretorio geral
dir_geral <- "D:/CEM_acessoSAN/"

# Definir diretorio de outputs e arquivos parciais
dir_outputs <- "3-outputs" # arquivos finais
dir_parcial <- "2-parcial" # arquivos parciais

# Listar municipios -----------------------------------------------------
municipios <- c(
  3550308, # São Paulo
  2507507, # João Pessoa
  3106200, # Belo Horizonte
  4314902,   # Porto Alegre
  1721000    # Palmas
)

# Municipios do Brasil --------------------------------------------------
municipios_br <- geobr::read_municipality(code_muni = "all", year = 2020, simplified = TRUE) %>%
  st_transform(4674)


# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, acesso, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  acesso <- st_read("acesso/tempo_minmed_3_estab.gpkg") %>%
    filter(!is.na(tempo_min_medio_3))
  cobertura_verde <- st_read("cobertura/cobertura_area_verde.gpkg")
  cobertura_agua <- st_read("cobertura/cobertura_agua.gpkg")
  
  cobertura <- rbind(
    cobertura_verde %>% mutate(classe = "area_verde"),
    cobertura_agua %>% mutate(classe = "agua")
  )
  
  # Filtrar município atual
  mun <- municipios_br %>% filter(code_muni == codigo)
  
  bbox_mun <- st_bbox(mun)
  
  expand_ratio <- 0.02  # 2% de cada lado
  bbox_exp <- bbox_mun
  bbox_exp["xmin"] <- bbox_mun["xmin"] - (bbox_mun["xmax"] - bbox_mun["xmin"]) * expand_ratio
  bbox_exp["xmax"] <- bbox_mun["xmax"] + (bbox_mun["xmax"] - bbox_mun["xmin"]) * expand_ratio
  bbox_exp["ymin"] <- bbox_mun["ymin"] - (bbox_mun["ymax"] - bbox_mun["ymin"]) * expand_ratio
  bbox_exp["ymax"] <- bbox_mun["ymax"] + (bbox_mun["ymax"] - bbox_mun["ymin"]) * expand_ratio
  fundo <- st_crop(municipios_br, bbox_exp)
  
  
  # CRS do município
  crs_mun <- st_crs(mun)
  
  # Definir CRS das camadas se ausente
  st_crs(hex) <- crs_mun
  
  # Transformar todas para CRS do município
  acesso <- st_transform(acesso, crs_mun)
  cobertura <- st_transform(cobertura, crs_mun)
  fundo <- st_transform(fundo, crs_mun)
  
  # Criar coluna est_cat
  acesso$est_cat <- NA_character_
  
  
  # Classificação
  # Definir intervalos fixos
  
  acesso$tempo_min_medio_3 <- round(acesso$tempo_min_medio_3)
  
  acesso$est_cat <- cut(
    acesso$tempo_min_medio_3,
    breaks = c(-Inf, 15, 30, 60, Inf),
    labels = c("0 - 15", "16 - 30", "31 - 60", "Mais de 60"),
    right = TRUE
  )
  
  # Garantir a ordem desejada
  acesso$est_cat <- factor(
    acesso$est_cat,
    levels = c("0 - 15", "16 - 30", "31 - 60", "Mais de 60")
  )
  
  pal_acesso <- c(
    "0 - 15"         = "#ffe0e0", 
    "16 - 30"   = "#ff9999", 
    "31 - 60"   = "#ff3333", 
    "Mais de 60"= "#990000"  
  )
  
  # Preparar cobertura filtrada
  cobertura_filtrada <- cobertura %>% 
    filter(classe %in% c("area_verde","agua")) %>%
    mutate(
      classe = case_when(
        classe == "area_verde" ~ "Áreas verdes",
        classe == "agua" ~ "Corpos d'água"
      ),
      classe = factor(classe, levels=c("Áreas verdes","Corpos d'água"))
    )
  
  # Paleta de cobertura
  pal_cobertura <- c("Áreas verdes"="#8FBC8F", "Corpos d'água"="lightblue")

  # Função para converter decimal -> grau minuto
  decimal_to_degmin <- function(x, is_lat = TRUE) {
    abs_x <- abs(x)
    deg <- floor(abs_x)
    min <- round((abs_x - deg) * 60, 1)  # 1 casa decimal nos minutos
    hemi <- if (is_lat) ifelse(x >= 0, "N", "S") else ifelse(x >= 0, "L", "O")
    paste0(deg, "°", min, "′", hemi)
  }
    
  # Criar mapa
  mapa <- ggplot() +
    
    # Fundo (outros municípios)
    geom_sf(data = fundo, aes(fill = "Municípios"), color = "bisque3") +
    scale_fill_manual(
      values = c("Municípios" = "beige"),
      name = " ",
      guide = guide_legend(
        order = 1,   # <<< ordem
        override.aes = list(color = "bisque3"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Hexágonos (áreas não caracterizadas)
    geom_sf(data = hex, aes(fill = "Áreas sem\ncaracterização"), color = "gray90") +
    scale_fill_manual(
      values = c("Áreas sem\ncaracterização" = "gray90"),
      name = " ",
      guide = guide_legend(
        order = 2,   # <<< ordem
        override.aes = list(color = "gray90"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Acesso (estabelecimentos)
    geom_sf(data = acesso, aes(fill = est_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Tempo médio\n(minutos)",
      guide = guide_legend(
        order = 4,   # <<< ordem
        ncol = 2)
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Cobertura (áreas verdes e corpos d'água)
    geom_sf(data = cobertura_filtrada, aes(fill = classe), color = NA) +
    scale_fill_manual(
      values = pal_cobertura,
      name = "Cobertura",
      guide = guide_legend(
        order = 3,   # <<< ordem
        ncol = 1)
    ) +
    
    coord_sf(
      expand = FALSE) +
    
    # Aplicar função para converter decimal -> grau minuto
    scale_x_continuous(
      name = NULL,
      labels = function(lon) decimal_to_degmin(lon, is_lat = FALSE)
    ) +
    scale_y_continuous(
      name = NULL,
      labels = function(lat) decimal_to_degmin(lat, is_lat = TRUE)
    ) +    
    # Barra de escala
    annotation_scale(
      location = "bl",
      width_hint = 0.3,
      text_cex = 0.6,
      line_width = 0.8
    ) +
    
    # Seta de norte
    annotation_north_arrow(
      location = "tl",
      which_north = "true",
      style = north_arrow_fancy_orienteering,
      height = unit(1, "cm"),
      width = unit(1, "cm")
    ) +
    
    theme_light() +
    theme(
      panel.background = element_rect(fill = "lightblue", color = NA),
      legend.position = "bottom",
      legend.box = "horizontal",
      legend.box.just = "left",
      legend.direction = "horizontal",
      legend.title = element_text(size = 10, face = "bold"),
      legend.text = element_text(size = 8)
    ) +
    labs(
      title = paste0(
        mun$name_muni, " (", mun$abbrev_state, ") - 2022:\n",
        "Média do tempo de caminhada para acesso\n",
        "aos 3 estabelecimentos de alimentação saudável mais próximos"
      ),
      subtitle = "Classificação Locais-Nova"
    ) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5, size = 14),
      plot.subtitle = element_text(face = "plain", hjust = 0.5, size = 14)
    )
  
  
  # Salvar
  pasta_saida <- file.path(dir_geral, dir_outputs, codigo)
  if(!dir.exists(pasta_saida)) dir.create(pasta_saida, recursive = TRUE)
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state, "_acesso_tempo_minmed_3_estab_classificacaoln.png"))
  
  # --- Salvar em A4 ---
  ggsave(
    filename = arquivo_mapa,
    plot = mapa,
    width = 21.0,
    height = 29.7,
    units = "cm",
    dpi = 1200
  )
}
