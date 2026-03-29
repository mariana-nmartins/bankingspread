library(readr)
library(tidyverse)
library(dplyr)
library(stringr)
library(purrr)
library(janitor)
library(httr)
library(jsonlite)
library(lubridate)

get_ifdata <- function(ano_inicio = 2012, ano_fim = 2024,
                       tipo_inst = 3, relatorio = "T") {
  
  anos <- ano_inicio:ano_fim
  meses <- c("03", "06", "09", "12")
  
  datas <- expand.grid(ano = anos, mes = meses) %>%
    mutate(AnoMes = paste0(ano, mes)) %>%
    pull(AnoMes)
  
  fetch <- function(anomes) {
    
    Sys.sleep(0.2)
    
    base_url <- paste0(
      "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/",
      "IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)",
      "?@AnoMes=", anomes,
      "&@TipoInstituicao=3",
      "&@Relatorio='T'",
      "&$format=json"
    )
    
    all_data <- list()
    url <- base_url
    
    repeat {
      
      res <- GET(url)
      
      if (status_code(res) != 200) {
        message("❌ Erro em ", anomes)
        return(NULL)
      }
      
      txt <- content(res, as = "text", encoding = "UTF-8")
      json <- fromJSON(txt, flatten = TRUE)
      
      all_data[[length(all_data) + 1]] <- json$value
      
      # 👇 chave da parada
      if (!is.null(json$`@odata.nextLink`)) {
        url <- json$`@odata.nextLink`
      } else {
        break
      }
    }
    
    out <- bind_rows(all_data)
    
    if (nrow(out) == 0) {
      message("⚠️ Sem dados em ", anomes)
      return(NULL)
    }
    
    out$AnoMes <- anomes
    return(out)
  }
  
  dados <- map_dfr(datas, fetch)
  
  return(dados)
}

dados_ifdata <- get_ifdata()

### a partir disso, "filtrar b1, inst ind", selecionar cnpj, filtrar ex (ativo e tal coisa),
#(resumo e n de agencias), anomes


###########

get_ifdata_cadastro <- function(ano_inicio = 2012, ano_fim = 2024) {
  
  # gerar AnoMes trimestral
  anos <- ano_inicio:ano_fim
  meses <- c("03", "06", "09", "12")
  
  datas <- expand.grid(ano = anos, mes = meses) %>%
    mutate(AnoMes = paste0(ano, mes)) %>%
    pull(AnoMes)
  
  # função para baixar um período
  fetch <- function(anomes) {
    
    Sys.sleep(0.2)  # 👈 evita travar API
    
    url <- paste0(
      "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata/",
      "IfDataCadastro(AnoMes=@AnoMes)?",
      "@AnoMes=", anomes,
      "&$top=10000&$format=json",
      "&$select=CodInst,Data,NomeInstituicao,DataInicioAtividade,Tcb,Td,Tc,SegmentoTb,Atividade,CnpjInstituicaoLider"
    )
    
    res <- GET(url)
    
    if (status_code(res) != 200) {
      message("Erro em ", anomes)
      return(NULL)
    }
    
    content(res, as = "text", encoding = "UTF-8") %>%
      fromJSON(flatten = TRUE) %>%
      .$value %>%
      mutate(AnoMes = anomes)
  }
  
  # baixar tudo e empilhar
  dados <- map_dfr(datas, fetch)
  
  return(dados)
}

cadastro <- get_ifdata_cadastro()



#############

cadastro_clean <- cadastro %>% select(CodInst, NomeInstituicao,Tcb,Td,CnpjInstituicaoLider) %>% 
  filter(Tcb == "B1", Td=="I")

banks_data = merge(dados_ifdata,cadastro_clean, by ="CodInst") %>% 
  select("AnoMes","CodInst","CnpjInstituicaoLider", "NomeInstituicao","NomeRelatorio",
         "NumeroRelatorio","Grupo","NomeColuna","Saldo")


saveRDS(banks_data, "banks_data")



####### O QUE FAZER EM RELACAO AO N DE AGENCIAS????
bank1 <- banks_data %>% 
  filter(NomeColuna %in% c(
    # Ativo total (base de tudo)
    "Ativo Total",
    
    # NIM
    "Resultado de Intermediação Financeira \n(c) = (a) + (b)",
    
    # Profit / TA
    "Resultado antes da Tributação, Lucro e Participação \n(g) = (e) + (f)",
    
    # Lucro Líquido / TA
    
    "Lucro Líquido",
    
    # Equity / TA
    "Patrimônio Líquido",
    
    # Loan / TA
    "Operações de Crédito Líquidas de Provisão \n(d)",
    
    # Overhead / TA
    "Despesas de Pessoal \n(d3)",
    "Despesas Administrativas \n(d4)",
    
    #Desp Trib
    
    "Despesas Tributárias \n(d5)",
    
    # Funding / TA
    "Depósito Total \n(a)",
    "Obrigações por Operações Compromissadas \n(b)",
    "Recursos de Aceites e Emissão de Títulos \n(c)",
    "Obrigações por Empréstimos e Repasses \n(d)",
    "Captações",
    
    # Non-interest earning assets / TA
    "Disponibilidades \n(a)",
    "Aplicações Interfinanceiras de Liquidez \n(b)"
  ))

bankwhat <- unique(bank1)

bank_wide <- bankwhat %>%  
  select(AnoMes, CodInst, NomeInstituicao, NomeColuna, Saldo) %>% unique() %>%
  pivot_wider(
    names_from = NomeColuna, 
    values_from = Saldo
  ) %>% na.omit()

### acho que tem que add LUCRO LIQUIDO
bank3 <- bank_wide %>%
  mutate(
    nim = `Resultado de Intermediação Financeira \n(c) = (a) + (b)` / `Ativo Total`,
    profit_ta = `Resultado antes da Tributação, Lucro e Participação \n(g) = (e) + (f)` / `Ativo Total`,
    equity_ta = `Patrimônio Líquido` / `Ativo Total`,
    loan_ta = `Operações de Crédito Líquidas de Provisão \n(d)` / `Ativo Total`,
    overhead_ta = (`Despesas de Pessoal \n(d3)` + `Despesas Administrativas \n(d4)`) / `Ativo Total`,
    trib_ta = (`Despesas Tributárias \n(d5)`)/ `Ativo Total`,
    netprofit_ta = `Lucro Líquido` / `Ativo Total`,
    funding_ta = (
      `Depósito Total \n(a)` +
        `Obrigações por Operações Compromissadas \n(b)` +
        `Recursos de Aceites e Emissão de Títulos \n(c)` +
        `Obrigações por Empréstimos e Repasses \n(d)` +
        `Captações`
    ) / `Ativo Total`,
    non_interest_ta = (
      `Disponibilidades \n(a)` +
        `Aplicações Interfinanceiras de Liquidez \n(b)`
    ) / `Ativo Total`
  ) %>% na.omit()

bank3 <- bank3 %>% select(c("AnoMes","CodInst","NomeInstituicao",
                            "nim", "profit_ta", "equity_ta", "loan_ta", 
                            "overhead_ta", "trib_ta","netprofit_ta", 
                            "funding_ta", "non_interest_ta"))

bank4 <- bank3 %>%
  select(-NomeInstituicao) %>%
  mutate(date = ymd(paste0(AnoMes, "01"))) %>% select(-AnoMes) %>% 
  rename(cnpj = CodInst)

###posso fazer dummy com esse prazo!!!
interest_rates_PJ2 <- interest_rates_PJ %>%  
  filter(modalidade == "Capital de giro com prazo superior a 365 dias - Pré-fixado")%>%  
  select(c(date,cnpj,spread_aa,spread_am)) 

dadoPJ <- full_join(bank4,interest_rates_PJ2, by = c("date","cnpj"))  
dadoPJ2 <- dadoPJ %>% select(-cnpj,-date) %>% na.omit() %>% select(-spread_aa)

###RESOLVER SOBRE TAXAS AA E AM (MENSALIZO OU PERCO O TRIM SLA)
#falta resto, deliquency etc
library(glmnet)
omg <- lm(spread_am ~ ., data = dadoPJ2)
summary(omg)



bank_monthly <- bank4 %>%
  arrange(cnpj, date) %>%
  group_by(cnpj) %>%
  # repete cada linha 3 vezes (para os 3 meses do trimestre)
  uncount(weights = 3, .id = "mes_no_trimestre") %>%
  # ajusta a data para o mês correto
  mutate(
    date = date %m+% months(mes_no_trimestre - 1),
    # divide os valores trimestrais por 3 para cada mês
    across(
      c(nim, profit_ta, equity_ta, loan_ta, overhead_ta, trib_ta, netprofit_ta, funding_ta, non_interest_ta),
      ~ ./3
    )
  ) %>%
  ungroup() %>%
  select(-mes_no_trimestre)


dadoPJ <- merge(bank_monthly,interest_rates_PJ2, by = c("date","cnpj")) ### ver se aqui é left ou right, merge
dadoPJ2 <- dadoPJ %>% select(-cnpj,-date,-spread_aa)

###RESOLVER SOBRE TAXAS AA E AM (MENSALIZO OU PERCO O TRIM SLA)
#falta resto, deliquency etc
#### tirar o CDI da taxa mensal!!!!!!


library(glmnet)
omg_micro <- lm(spread_am ~ ., data = dadoPJ2)
summary(omg_micro)


####### aliquota do comp
req_reserves <- readxl::read_xls("compulsorios_new.xls")

###### IBC-br
url_ibc <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.24364/dados?formato=json"

# Ler o JSON e transformar corretamente a data
ibc <- fromJSON(content(GET(url_ibc), "text", encoding="UTF-8")) %>%
  mutate(
    date = dmy(data),  # converte "dd/mm/yyyy" para Date
    ibc = as.numeric(valor)  # garante que o valor seja numérico
  ) %>%
  select(date, ibc) %>%
  filter(date <= ymd("2024-12-01"), date >= ymd("2011-12-01")) %>% 
  mutate(ibc = ibc/lag(ibc) -1) %>% na.omit()

###### IPCA
url_ipca <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.433/dados?formato=json"
ipca <- fromJSON(content(GET(url_ipca), "text", encoding="UTF-8")) %>%
  mutate(
    date = dmy(data),  # converte "dd/mm/yyyy" para Date
    ipca = as.numeric(valor)  # garante que o valor seja numérico
  ) %>%
  select(date, ipca) %>%
  filter(date <= ymd("2024-12-01"), date >= ymd("2012-01-01"))  # data final


###### Inadimplência
# URLs das séries
url_inad_pj <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21086/dados?dataInicial=01/01/2012&dataFinal=31/12/2024&formato=json"
url_inad_pf <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.21114/dados?dataInicial=01/01/2012&dataFinal=31/12/2024&formato=json"

# Pessoa Jurídica
inad_pj <- fromJSON(content(GET(url_inad_pj), "text", encoding="UTF-8")) %>%
  mutate(
    date = dmy(data),
    inad_pj_pct = as.numeric(valor)
  ) %>%
  select(date, inad_pj_pct)

# Pessoa Física
inad_pf <- fromJSON(content(GET(url_inad_pf), "text", encoding="UTF-8")) %>%
  mutate(
    date = dmy(data),
    inad_pf_pct = as.numeric(valor)
  ) %>%
  select(date, inad_pf_pct)

###selic
url_selic <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4390/dados?dataInicial=01/01/2012&dataFinal=31/12/2024&formato=json"

selic_monthly <- fromJSON(content(GET(url_selic), "text", encoding="UTF-8")) %>%
  mutate(
    date = dmy(data),
    selic_pct = as.numeric(valor)
  ) %>%
  select(date, selic_pct) %>% 
  filter(date <= ymd("2024-12-01"), date >= ymd("2012-01-01")) 

dadoPJ <- left_join(dadoPJ,selic_monthly) %>% 
  mutate(spread_am = spread_am-selic_pct)


##### AINDA FALTA O HHI!!!!
##### PRECISO TORNAR TUDO VARIAÇÃO? OU TORNO TUDO ÍNDICE?
#### POR QUE TEM TANTO NA???????????????
###rerodar tudo!!!!
### usar selic, ipca lagged?
#fazer trimestral?

dadoPJ3 = left_join(dadoPJ,req_reserves)
dadoPJ3 <- left_join(dadoPJ3,inad_pj)
dadoPJ3 <- left_join(dadoPJ3,ipca)
dadoPJ3 <- left_join(dadoPJ3,ibc) %>% select(-c(cnpj,date, spread_aa, selic_pct))
######KUNT USA YEARLY DATA

omg <- lm(spread_am ~ ., data = dadoPJ3)
summary(omg)

omg_macro <- lm(spread_am ~ short_term_dep+ipca+ibc+inad_pj_pct+time_deposits, data = dadoPJ3)
summary(omg_macro)
