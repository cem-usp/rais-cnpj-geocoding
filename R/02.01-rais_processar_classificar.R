# -------------------------- #
# Tratar dados da RAIS e
# classificar estabelecimentos
# como saudaveis
# -------------------------- #

# Nota: foi disponibilizada ao grupo 
# de pesquisa a base completa da RAIS
# para o ano de 2022

# Nao disponibilizaremos a base por 
# motivos legais de permissao de uso

# Observar calculo realizado na etapa
# 03, que utiliza o CNPJ brasileiro

# Bibliotecas ----
library(arrow) # arquivos .parquet
library(stringr) # trabalhos com string
library(dplyr) # operacoes gerais
library(fs) # para dir_create
library(data.table) 

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

# tornar um dataframe com correspondencias a 6 digitos
municipios <- tibble(
  co_municipio = as.character(municipios)
) %>%
  mutate(
    co_municipio6 = str_sub(co_municipio, 1, 6)
  )

# Ler e processar dados de cnae selecionados ----------------------------
# Nota: classificacao adotada conforme notas metodologicas
cnae <- openxlsx::read.xlsx('1-inputs/20250917_acesso_locais-nova_CaN.xlsx') %>%
  mutate(
    uf   = sigla_uf,
    cnae = No.CNAE,
    g1_g2_uf = g1g2_uf
  ) %>%
  dplyr::select(cnae, uf, g0_uf, g1_g2_uf, g3_uf, g4_uf)                        

# Ler e processar dados de de uf ----------------------------------------
# Nota: arquivo ja tratado anteriormente
uf <- readxl::read_xlsx(path = '1-inputs/uf_tratado.xlsx') %>%
  mutate(porte_pop = factor(
    x = porte_pop, levels = c(1, 2, 3, 4, 5),
    labels = c('Até 50 mil',
               'De 50.001 até 100.000',
               'De 100.001 até 200.000',
               'De 200.001 até 500.000',
               'Mais de 500 mil'),
    ordered = T)
    ) %>%
  rename(
    no_municipio = name_muni,
    co_municipio6 = code_6,
    uf = abbrev_state,
  ) %>%
   dplyr::select(co_municipio6, uf, no_municipio, porte_pop
         )

# Processar arquivo da RAIS (2022) --------------------------------------
rais <- arrow::read_parquet("1-inputs/RAIS/rais_alim_2022.parquet")

# Filtrar estabelecimentos que nao estejam ativos
rais <- rais %>%
  filter(in_atividade_ano == 9) %>%
  filter(in_rais_negativa == 0 | (in_rais_negativa == 1 & qt_vinculos_ativos > 0))

# Traduzir porte do estabelecimento
rais <- rais %>% 
  mutate(
    in_tamanho_estab = as.numeric(in_tamanho_estab),
    tamanho_estab = case_when(
      in_tamanho_estab <= 3 ~ "Até 9",
      in_tamanho_estab %in% c(4, 5) ~ "10 a 49",
      in_tamanho_estab >= 6 ~ "50 ou mais"
    ),
    tamanho_estab = factor(tamanho_estab,
                           levels = c("Até 9", "10 a 49", "50 ou mais"))
  )


# Padronizar codigos de municipio, cnae e cnpj
rais <- rais %>%
  mutate(
    cnae = sprintf("%07d", as.integer(co_cnae20_subclasse)), # CNAE com 7 dígitos
    cnpj = str_pad(as.character(nu_cnpj_cei), 14, pad = "0"),          # CNPJ 14 dígitos
    co_municipio6 = as.character(co_municipio)                # mantém como string
  ) %>%
   dplyr::select(cnae, cnpj, co_municipio6, qt_vinculos_ativos, tamanho_estab)


# Juntar as tabelas de uf e rais
rais <- rais %>%
  left_join(uf, by = c("co_municipio6"))

# Loop com filtragem por municipio
for (codigo in municipios$co_municipio) {
  
  # Pegar o code_6 correspondente
  codigo6 <- municipios %>%
    filter(co_municipio == codigo) %>%
    pull(co_municipio6)
  
  # Criar diretorio
  dir_saida <- file.path(dir_parcial, codigo, "rais")
  dir.create(dir_saida, recursive = TRUE, showWarnings = FALSE)
  
  # Filtrar RAIS so do municipio
  rais_codigo <- rais %>%
    filter(co_municipio6 == codigo6)
  
  # Filtrar por uf e por cnae
  rais_codigo <- rais_codigo %>%
    semi_join(cnae, by = c("cnae", "uf"))
  
  # Processar apenas saudaveis
  
  rais_g0 <- rais_codigo %>%
    semi_join(filter(cnae, g0_uf == 1), by = c("cnae", "uf"))
  arrow::write_parquet(rais_g0, file.path(dir_saida, "rais_g0.parquet"))
  
  # 1) Processar saudaveis
  rais_g1_g2 <- rais_codigo %>%
    semi_join(filter(cnae, g1_g2_uf == 1), by = c("cnae", "uf"))
  arrow::write_parquet(rais_g1_g2, file.path(dir_saida, "rais_g1_g2.parquet"))
  
  # 2) Processar g3
  rais_g3 <- rais_codigo %>%
    semi_join(filter(cnae, g3_uf == 1), by = c("cnae", "uf"))
  arrow::write_parquet(rais_g3, file.path(dir_saida, "rais_g3.parquet"))
  
  # 3) Processar g4
  rais_g4 <- rais_codigo %>%
    semi_join(filter(cnae, g4_uf == 1), by = c("cnae", "uf"))
  arrow::write_parquet(rais_g4, file.path(dir_saida, "rais_g4.parquet"))
}
