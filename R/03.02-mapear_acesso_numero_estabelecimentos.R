# ----------------------------------------- #
# Produzir mapa de numero de estabelecimentos
# acessiveis em ate 15 minutos
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
  3550308,   # Sao Paulo
  2507507,   # Joao Pessoa
  3106200,   # Belo Horizonte
  4314902,   # Porto Alegre
  1721000,   # Palmas
  5300108,   # Brasilia
  5208707    # Goiania
)

# Municipios do Brasil --------------------------------------------------
municipios_br <- geobr::read_municipality(code_muni = "all", year = 2020, simplified = TRUE) %>%
  st_transform(4674)

# ----------------------------------------- #
# Processar e mapear estabelecimentos g4
# ----------------------------------------- #

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, acesso, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  acesso <- st_read("rais/acesso/n_estab_15_g4.gpkg") %>%
    filter(!is.na(est_pop_15))
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
  
  # Garantir a ordem dos fatores
  acesso$est_cat <- NA_character_

  # Classificação
  # Definir intervalos e labels
  breaks <- c(-Inf, 0, 3, 5, 10, 15, Inf)
  labels <- c("0", "1 - 3", "4 - 5", "6 - 10", "11 - 15", "Mais de 15")
  
  acesso$est_cat <- cut(
    acesso$est_pop_15,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  acesso$est_cat <- factor(acesso$est_cat, levels = labels)
  
  # Paleta de cores ajustada
  pal_acesso <- c(
    "0"         = "#ffe0e0", 
    "1 - 3"    = "#ffb3b3", 
    "4 - 5"   = "#ff9999", 
    "6 - 10"   = "#ff6666", 
    "11 - 15"   = "#ff3333", 
    "Mais de 15"= "#990000"  
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
    geom_sf(data = hex, aes(fill = "Áreas sem\ncaracterização"), color = "gray80") +
    scale_fill_manual(
      values = c("Áreas sem\ncaracterização" = "gray80"),
      name = " ",
      guide = guide_legend(
        order = 2,   # <<< ordem
        override.aes = list(color = "gray80"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Acesso (estabelecimentos)
    geom_sf(data = acesso, aes(fill = est_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Estabelecimentos\npor 1000 habitantes",
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
        "Densidade de estabelecimentos comerciais do Grupo 4\n",
        "acessíveis em até 15 minutos de caminhada"
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
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_acesso_n_estab_15min_g4.png"))

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


# ------------------------------------------- #
# Processar e mapear estabelecimentos g4
# ------------------------------------------- #

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, acesso, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  acesso <- st_read("rais/acesso/n_estab_15_g3.gpkg") %>%
    filter(!is.na(est_pop_15))
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
  
  # Garantir a ordem dos fatores
  acesso$est_cat <- NA_character_
  
  # Classificação
  # Definir intervalos e labels
  breaks <- c(-Inf, 0, 3, 5, 10, 15, Inf)
  labels <- c("0", "1 - 3", "4 - 5", "6 - 10", "11 - 15", "Mais de 15")
  
  acesso$est_cat <- cut(
    acesso$est_pop_15,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  acesso$est_cat <- factor(acesso$est_cat, levels = labels)
  
  # Paleta de cores ajustada
  pal_acesso <- c(
    "0"          = "#ffd9b3", 
    "1 - 3"      = "#fdae6b", 
    "4 - 5"      = "#fd8d3c", 
    "6 - 10"     = "#f16913",  
    "11 - 15"    = "#d94801", 
    "Mais de 15" = "#7f2704"  
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
    geom_sf(data = hex, aes(fill = "Áreas sem\ncaracterização"), color = "gray80") +
    scale_fill_manual(
      values = c("Áreas sem\ncaracterização" = "gray80"),
      name = " ",
      guide = guide_legend(
        order = 2,   # <<< ordem
        override.aes = list(color = "gray80"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Acesso (estabelecimentos)
    geom_sf(data = acesso, aes(fill = est_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Estabelecimentos\npor 1000 habitantes",
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
        "Densidade de estabelecimentos comerciais do Grupo 3\n",
        "acessíveis em até 15 minutos de caminhada"
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
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_acesso_n_estab_15min_g3.png"))
  
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


# ------------------------------------------- #
# Processar e mapear estabelecimentos g1 e g2
# ------------------------------------------- #

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, acesso, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  acesso <- st_read("rais/acesso/n_estab_15_g1_g2.gpkg") %>%
    filter(!is.na(est_pop_15))
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
  
  # Garantir a ordem dos fatores
  acesso$est_cat <- NA_character_
  
  # Classificação
  # Definir intervalos e labels
  breaks <- c(-Inf, 0, 3, 5, 10, 15, Inf)
  labels <- c("0", "1 - 3", "4 - 5", "6 - 10", "11 - 15", "Mais de 15")
  
  acesso$est_cat <- cut(
    acesso$est_pop_15,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  acesso$est_cat <- factor(acesso$est_cat, levels = labels)
  
  # Paleta de cores ajustada
  pal_acesso <- c(
    "0"          = "#3F007D",  
    "1 - 3"      = "#6A00A8", 
    "4 - 5"      = "#9C179E",  
    "6 - 10"     = "#BC5090",  
    "11 - 15"    = "#E09FDD",  
    "Mais de 15" = "#F7E6F7"  
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
    geom_sf(data = hex, aes(fill = "Áreas sem\ncaracterização"), color = "gray80") +
    scale_fill_manual(
      values = c("Áreas sem\ncaracterização" = "gray80"),
      name = " ",
      guide = guide_legend(
        order = 2,   # <<< ordem
        override.aes = list(color = "gray80"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Acesso (estabelecimentos)
    geom_sf(data = acesso, aes(fill = est_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Estabelecimentos\npor 1000 habitantes",
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
        "Densidade de estabelecimentos comerciais dos grupos 1 e 2\n",
        "acessíveis em até 15 minutos de caminhada"
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
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_acesso_n_estab_15min_g1_g2.png"))
  
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

# ------------------------------------------- #
# Processar e mapear estabelecimentos g0
# ------------------------------------------- #

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, acesso, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  acesso <- st_read("rais/acesso/n_estab_15_g0.gpkg") %>%
    filter(!is.na(est_pop_15))
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
  
  # Garantir a ordem dos fatores
  acesso$est_cat <- NA_character_
  
  # Classificação
  # Definir intervalos e labels
  breaks <- c(-Inf, 0, 3, 5, 10, 15, Inf)
  labels <- c("0", "1 - 3", "4 - 5", "6 - 10", "11 - 15", "Mais de 15")
  
  acesso$est_cat <- cut(
    acesso$est_pop_15,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  acesso$est_cat <- factor(acesso$est_cat, levels = labels)
  
  # Paleta de cores ajustada
  pal_acesso <- c(
    "0"          = "#980043",  
    "1 - 3"      = "#dd1c77", 
    "4 - 5"      = "#df65b0",  
    "6 - 10"     = "#c994c7",  
    "11 - 15"    = "#d4b9da",  
    "Mais de 15" = "#f1eef6"  
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
    geom_sf(data = hex, aes(fill = "Áreas sem\ncaracterização"), color = "gray80") +
    scale_fill_manual(
      values = c("Áreas sem\ncaracterização" = "gray80"),
      name = " ",
      guide = guide_legend(
        order = 2,   # <<< ordem
        override.aes = list(color = "gray80"))
    ) +
    
    ggnewscale::new_scale_fill() +   # <<< reset
    
    # Acesso (estabelecimentos)
    geom_sf(data = acesso, aes(fill = est_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Estabelecimentos\npor 1000 habitantes",
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
        "Densidade de estabelecimentos comerciais no Grupo 0\n",
        "acessíveis em até 15 minutos de caminhada"
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
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_acesso_n_estab_15min_g0.png"))
  
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

