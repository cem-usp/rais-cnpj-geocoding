# --------------------------------- #
# Produzir geometrias hexagonais para
# pontos de acesso a estabelecimentos
# --------------------------------- #

# Bibliotecas ----
library(arrow)     # leitura de arquivos .parquet
library(sf)        # operações de geometria
library(ggplot2)   # Fazer mapas
library(dplyr)     # manipulação de dados
library(sfarrow)   # leitura de parquet com geometria
library(geobr)     # Ler geometrias de municipios
library(gridExtra)
library(grid)
library(ggnewscale)
library(cowplot)
library(ggspatial)

# Definir diretorio de trabalho -----------------------------------------
setwd("D:/CEM_acessoSAN/")

dir_inputs <- "1-inputs" # arquivos de input
dir_parcial <- "2-parcial" # arquivos intermediarios
dir_outputs <- "3-outputs" # arquivos finais

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
grupos <- c("g0", "g4")

# Calcular g4 sobre g0 -------------------------------------------------------

for (mun in municipios) {
  
  cat("Processando município:", mun, "\n")
  
  # Criar pasta 'relacoes_grupos'
  dir_relacoes <- file.path(dir_parcial, mun, "rais/relacoes_grupos")
  if (!dir.exists(dir_relacoes)) dir.create(dir_relacoes, recursive = TRUE)
  
  # Ler hexagonos para municipio ---
  caminho_hex <- file.path(dir_parcial, mun, "hex/hex_urbanizado.parquet")
  hex <- sfarrow::st_read_parquet(caminho_hex)
  
  resultados <- list()
  
  for (grupo in grupos) {
    
    cat(" -> Grupo:", grupo, "\n")
    
    caminho_ttm <- file.path(dir_parcial, mun, "rais/tempos", paste0("ttm_", grupo, ".rds"))
    ttm <- readRDS(caminho_ttm)
    
    # Número de estabelecimentos até 15 minutos
    n_acesso_15 <- ttm %>%
      mutate(tempo_15 = ifelse(travel_time_p50 <= 15, 1, 0)) %>%
      group_by(from_id) %>%
      summarise(est_15 = sum(tempo_15, na.rm = TRUE), .groups = "drop") %>%
      # renomear a coluna "est_15" criando um novo nome dinamico (ex: "est_15_g0" ou "est_15_g4") por 'grupo'
      rename(!!paste0("est_15_", grupo) := est_15) 
    
    resultados[[grupo]] <- n_acesso_15
  }
  
  # Juntar resultados g0 e g4
  relacao <- resultados[["g0"]] %>%
    left_join(resultados[["g4"]], by = "from_id") %>%
    mutate(
      rel_g0_g4 = ifelse(est_15_g4 > 0, est_15_g0 / est_15_g4, NA_real_)
    )
  
  # Checar o resultado 'esquisito' de nao existir lugares com mais g0 que g4, no maximo igual
  cat("Máximo rel_g0_g4 no município", mun, ":", 
      max(relacao$rel_g0_g4, na.rm = TRUE), "\n")
  
  # Entender isso ai
  # 1. Mostrar primeiros 10 hexágonos g0/g4 > 1
  cat("   Hexágonos com rel_g0_g4 > 1 no município", mun, ":\n")
  print(
    relacao %>%
      filter(rel_g0_g4 > 1) %>%
      arrange(desc(rel_g0_g4)) %>%
      head(10)
  )
  
  # 2. Mostrar totais de estabelecimentos g0 e g4
  cat("   Totais no município", mun, ":\n")
  print(
    relacao %>%
      summarise(
        total_g0 = sum(est_15_g0, na.rm = TRUE),
        total_g4 = sum(est_15_g4, na.rm = TRUE)
      )
  )
  
  # 3. Contar quantos hexágonos têm rel_g0_g4 exatamente igual a 1
  cat("   Hexágonos com rel_g0_g4 = 1 no município", mun, ":\n")
  print(
    relacao %>%
      summarise(
        hex_rel_1 = sum(rel_g0_g4 == 1, na.rm = TRUE)
      )
  )
  
  # Criar histograma
  hist_plot <- ggplot(relacao, aes(x = rel_g0_g4)) +
    geom_histogram(binwidth = 0.1, fill = "skyblue", color = "black") +
    labs(
      title = paste("Histograma de rel_g0_g4 - Município", mun),
      x = "g0 / g4",
      y = "Número de hexágonos"
    ) 
  
  # Salvar histograma em PNG na pasta relacoes_grupos
  arquivo_hist <- file.path(dir_relacoes, paste0("hist_rel_g0_g4_", mun, ".png"))
  ggsave(arquivo_hist, plot = hist_plot, width = 7, height = 5)
  
  # Proceder mesmo com duvidas ---------------------------------------------------
  # Juntar com hexágonos para salvar como geometria
  relacao_hex <- hex %>%
    left_join(relacao, by = c("h3_address" = "from_id"))
  
  # Salvar resultado
  write_sf(
    relacao_hex,
    file.path(dir_relacoes, "relacao_g0_g4.gpkg")
  )
    
    cat("Concluído município:", mun, "\n")
}

# Prompt do resultado -------------------------------------------------
# SEM INFOS DE 'ENTENDIMENTO' DO PROCESSO
# Processando município: 3550308 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 3550308 : 1 
# Concluído município: 3550308 
# Processando município: 2507507 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 2507507 : 2.833333 
# Concluído município: 2507507 
# Processando município: 3106200 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 3106200 : 1 
# Concluído município: 3106200 
# Processando município: 4314902 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 4314902 : 1 
# Concluído município: 4314902 
# Processando município: 1721000 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 1721000 : 1 
# Concluído município: 1721000 
# Processando município: 5300108 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 5300108 : 1 
# Concluído município: 5300108 
# Processando município: 5208707 
# -> Grupo: g0 
# -> Grupo: g4 
# Máximo rel_g0_g4 no município 5208707 : 1 
# Concluído município: 5208707 
# CONCLUSAO: somente João Pessoa tem casos de mais g0 que g4
# POR QUE? não sei

# ----------------------------------------- #
# Produzir mapa de relacão g0 sobre g4
# ----------------------------------------- #

# Municipios do Brasil --------------------------------------------------
municipios_br <- geobr::read_municipality(code_muni = "all", year = 2020, simplified = TRUE) %>%
  st_transform(4674)

dir_geral <- "D:/CEM_acessoSAN/"

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, relacao_hex, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  relacao_hex <- st_read("rais/relacoes_grupos/relacao_g0_g4.gpkg")
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
  
  # Transformar todas para CRS do município (exceto relacao_hex para manter os NAs)
  cobertura <- st_transform(cobertura, crs_mun)
  fundo <- st_transform(fundo, crs_mun)
  
  # Garantir a ordem dos fatores
  relacao_hex$rel_cat <- NA_character_
  
  # Classificação
  # Definir intervalos e labels
  breaks <- c(-Inf, 0, .5, 1, Inf)
  labels <- c(
    "Sem acesso a\nestabelecimentos de Grupo 0",
    "Grupo 0 representa até\nmetade de Grupo 4",
    "Grupo 0 representa entre\nmetade e total de Grupo 4",
    "Maior quantidade de Grupo 0\nem relação a Grupo 4"
  )  
  
  relacao_hex$rel_cat <- cut(
    relacao_hex$rel_g0_g4,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  # substituir NA por categoria a parte
  relacao_hex$rel_cat <- addNA(relacao_hex$rel_cat)
  levels(relacao_hex$rel_cat) <- c(levels(relacao_hex$rel_cat), "Sem acesso aos grupos")
  relacao_hex$rel_cat[is.na(relacao_hex$rel_cat)] <- "Sem acesso aos grupos"
  
  # garantir ordem dos fatores (inclui o novo nível por último)
  relacao_hex$rel_cat <- factor(relacao_hex$rel_cat, levels = c(labels, "Sem acesso aos grupos"))
  

  # Paleta de cores ajustada
  pal_acesso <- c(
    "Maior quantidade de Grupo 0\nem relação a Grupo 4"    = "#fee5d9", 
    "Grupo 0 representa entre\nmetade e total de Grupo 4"   = "#fcae91", 
    "Grupo 0 representa até\nmetade de Grupo 4"   = "#fb6a4a", 
    "Sem acesso a\nestabelecimentos de Grupo 0" = "#cb181d", 
    "Sem acesso aos grupos"= "#6A00A8"  
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
    geom_sf(data = relacao_hex, aes(fill = rel_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Relação de\ngrupo 0\nsobre grupo 4",
      guide = guide_legend(
        order = 4,   # <<< ordem
        ncol = 1)
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
        "Estabelecimentos acessiveis de Grupo 0 em relação\n",
        "a estebelecimentos acessíveis de Grupo 4"
      ),
      subtitle = "Classificação Locais-Nova"
    ) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5, size = 14),
      plot.subtitle = element_text(face = "plain", hjust = 0.5, size = 14)
    )
  
  
  # Salvar
  pasta_saida <- file.path(dir_geral, dir_outputs
                           #, codigo
                           )
  if(!dir.exists(pasta_saida)) dir.create(pasta_saida, recursive = TRUE)
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_mapa_g0_sobre_g4.png"))
  
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

# ----------------------------------------- #
# Detalhar estabelecimentos g0 sobre g4
# ----------------------------------------- #

# --- Loop por município ---
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Ler hex, relacao_hex, cobertura
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  relacao_hex <- st_read("rais/relacoes_grupos/relacao_g0_g4.gpkg")
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
  
  # Transformar todas para CRS do município (exceto relacao_hex para manter os NAs)
  cobertura <- st_transform(cobertura, crs_mun)
  fundo <- st_transform(fundo, crs_mun)
  
  # Garantir a ordem dos fatores
  relacao_hex$rel_cat <- NA_character_
  
  # Criar breaks menores
  breaks <- c(-Inf, 0, 0.2, 0.4, 0.6, 0.8, 1, Inf)
  
  # Labels correspondentes
  labels <- c(
    "0",                # rel = 0
    "0 - 0,2",
    "0,2 - 0,4",
    "0,4 - 0,6",
    "0,6 - 0,8",
    "0,8 - 1",
    ">1"                # rel > 1
  )
  
  # Classificar rel_g0_g4
  relacao_hex$rel_cat <- cut(
    relacao_hex$rel_g0_g4,
    breaks = breaks,
    labels = labels,
    include.lowest = TRUE,
    right = TRUE
  )
  
  # Substituir NAs por categoria a parte
  relacao_hex$rel_cat <- addNA(relacao_hex$rel_cat)
  levels(relacao_hex$rel_cat) <- c(levels(relacao_hex$rel_cat), "Sem acesso aos grupos")
  relacao_hex$rel_cat[is.na(relacao_hex$rel_cat)] <- "Sem acesso aos grupos"
  
  # Garantir ordem dos fatores
  relacao_hex$rel_cat <- factor(
    relacao_hex$rel_cat,
    levels = c(labels, "Sem acesso aos grupos")
  )
  
  # Paleta de cores para as novas classes (mantendo o último nível igual)
  pal_acesso <- c(
    "0"                        = "#99000d",
    "0 - 0,2"                   = "#cb181d",
    "0,2 - 0,4"                 = "#ef3b2c",
    "0,4 - 0,6"                 = "#fb6a4a",
    "0,6 - 0,8"                 = "#fc9272",
    "0,8 - 1"                    = "#fcbba1",
    ">1"                        = "#fee5d9",
    "Sem acesso aos grupos"     = "#6A00A8"
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
    geom_sf(data = relacao_hex, aes(fill = rel_cat), color = NA) +
    scale_fill_manual(
      values = pal_acesso,
      name = "Relação de\ngrupo 0\nsobre grupo 4",
      guide = guide_legend(
        order = 4,   # <<< ordem
        ncol = 1)
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
        "Estabelecimentos acessiveis de Grupo 0 em relação\n",
        "a estebelecimentos acessíveis de Grupo 4"
      ),
      subtitle = "Classificação Locais-Nova"
    ) +
    theme(
      plot.title = element_text(face = "bold", hjust = 0.5, size = 14),
      plot.subtitle = element_text(face = "plain", hjust = 0.5, size = 14)
    )
  
  
  # Salvar
  pasta_saida <- file.path(dir_geral, dir_outputs, "rel_detalhado"
                           #, codigo
  )
  if(!dir.exists(pasta_saida)) dir.create(pasta_saida, recursive = TRUE)
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state,  "_mapa_g0_sobre_g4_detalhado.png"))
  
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

# Verificar somente casos de mesma quantidade

# Loop para todos os municípios
for (codigo in municipios) {
  
  setwd(file.path(dir_geral, dir_parcial, codigo))
  
  # Filtrar município
  mun <- municipios_br %>% filter(code_muni == codigo)
  
  hex <- read_parquet("hex/hex.parquet") %>% st_as_sf()
  relacao_hex <- st_read("rais/relacoes_grupos/relacao_g0_g4.gpkg")
  
  # Filtrar apenas hexágonos com rel_g0_g4 == 1
  hex_mesma_quant <- relacao_hex %>%
    filter(rel_g0_g4 == 1) %>%
    mutate(
      categoria_qtd = case_when(
        est_15_g0 <= 3 ~ "Até 3 3",
        est_15_g0 > 3  ~ "Acima de 3 3",
        TRUE           ~ NA_character_
      )
    )
  
  hex_mesma_quant$categoria_qtd <- factor(
    hex_mesma_quant$categoria_qtd,
    levels = c("Até 3 3", "Acima de 3 3")
  )
  
  mapa <- ggplot() +
    geom_sf(data = mun, fill = NA, color = "black") +
    geom_sf(data = hex_mesma_quant, aes(fill = categoria_qtd), color = NA) +
    scale_fill_manual(values = c("Até 3 3" = "red", "Acima de 3 3" = "purple")) +
    labs(
      title = "Hexágonos com mesma quantidade de g0 e g4",
      subtitle = paste("Município:", mun$name_muni),
      fill = "Quantidade de estabelecimentos"
    )
  
  # Criar pasta de saída
  pasta_saida <- file.path(dir_geral, dir_outputs, "rel_detalhado")
  if(!dir.exists(pasta_saida)) dir.create(pasta_saida, recursive = TRUE)
  
  # Salvar mapa
  arquivo_mapa <- file.path(pasta_saida, paste0(codigo, "_", mun$abbrev_state, "_hex_g0_eq_g4.png"))
  ggsave(arquivo_mapa, mapa, width = 21, height = 29.7, units = "cm", dpi = 300)
  
  cat("Mapa salvo para município:", codigo, "\n")
}
