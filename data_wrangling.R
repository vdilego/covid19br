rm(list=ls())


library(tidyverse)
library(curl)
library(readxl)
library(lubridate)
library(forcats)
library(paletteer)
library(dplyr)
library(tidyr)
library(ggplot2)
library(janitor)
library(countrycode)
library(here)
library(readr)
library(curl)
library(data.table)
library(Hmisc)
library(ggrepel)
library(ungroup)
library(pacman)
library(doParallel)
library(fst)
library(plyr)
library(data.table)
library(doSNOW)
library(devtools)

# read in population estimates by city, age and sex

pop_city <- read_excel("Produto4-2010-2030-ProjMunic.xlsx")
pop_city<-pop_city[,-25]     # taking the total population out, keeping only age and sex

# Reshaping to long to perform analysis
names(pop_city)
pop_city_long<-pop_city %>%
  pivot_longer(c(6:24),
  names_to = "Age", values_to = "Population" ) %>%
  filter(Ano==2020) %>%
  group_by(ARmaior,Armenor,NomeMunic,Age) %>%
  dplyr::summarise(Total=sum(Population)) %>% # sum both men and women
  mutate(Age=factor(Age, levels=names(pop_city)[c(6:24)])) %>%
  dplyr::arrange(Age,  .by_group = TRUE)# %>%
  #separate(NomeMunic,c("City","UF"),sep = "([-])")        # separating the city from UF
# we get some problems with some cities but it does not matter as we will use the code to join.
# cities like Xique-Xique in bahia get lost in terms of UF.


# changing "a" to hyphen for age groups, to help in consistency issues
pop_city_long$Age<-str_replace(pop_city_long$Age, ".a.", "-")

#converting back to factor
pop_city_long$Age<-as_factor(pop_city_long$Age)

# ungrouping pop into single year ages using pclm
un.pclm <- function(x,z){

  # last bin interval
  nlast  <- 21

  #  exposure
  offset <- z
  # split exposure using pclm...
  off1   <- pclm(x = x, y = offset, nlast = nlast, control = list(lambda = 1 / 1e6))$fitted
  #off1   <- pclm(x = x, y = nyc$Pop, nlast = nlast, control = list(lambda = 1 / 1e6))$fitted
  # split counts using split exposure as offset returns rates

  Age<-0:110
  df <- data.frame(Age,off1)
  colnames(df) <- c("Age","Pop")
  return(df)
}

# setting data.table as it is easier to apply the function
setDT(pop_city_long)

# Ages
x<-seq(0,90,by=5)

# takin out hideous scientific notation at reporting rates
options(scipen=999)

#apply function for estimating pclm into single ages by city code, city name and state
pop_city_grad<-pop_city_long[, un.pclm(x,Total), by=.(Armenor,NomeMunic)]

# testing sums of all ages and comparing with total as sense checks
pop_city_grad_s<-pop_city_grad %>%          # it works I compared with the spreadsheet
  group_by(Armenor, NomeMunic) %>%
  dplyr::summarise(sum(Pop))

# graphs for checking both age distributions
#pop_5 <- ggplot(pop_city_long, aes(x = Age, y = Population, fill = Gender)) +
#  geom_bar(subset = .(Gender == "Female"), stat = "identity") +
#  geom_bar(subset = .(Gender == "Male"), stat = "identity") +
#  scale_y_continuous(breaks = seq(-4000000, 4000000, 1000000),
#                     labels = paste0(as.character(c(4:0, 1:4)), "m")) +
 # coord_flip() +
  #scale_fill_brewer(palette = "Set1") +
  #theme_bw()

# -----------------------------------------------------------------------------#
# Now bringing in the mortality rates from Bernardo´s life tables
# see that life table files have the same .csv pattern
# don´t forget to fread with encoding="UTF-8" to read in Brazilian accent
# huge files we need some more horse power on this one.
# pacman should do the trick. Let´s go parallel, baby!
# ------------------------------------------------------------------------------#

#install_github('nathanvan/parallelsugar')
# use parallel sugar for windows operational systems and parallel for mac os

library(parallelsugar)

csv.list <- list.files(path="C:/Users/vdile/Documents/Git/covid_city/life_tables_city",
                       pattern=".csv$", full.names=TRUE)
fn<-str_subset(csv.list,"\\.csv$")

# use parallel setting
(cl = detectCores() %>%
    makeCluster()) %>%
  registerDoParallel()

# read all files using fread from data.table and UTF-8 encoding and bind all together
system.time({
  big_df = foreach(i = fn,
                   .packages = "data.table") %dopar% {
                     fread(i,colClasses = "character", encoding="UTF-8")
                   } %>%
    rbindlist(idcol=T)
})

# end of parallel work
stopImplicitCluster()

#install.packages("AmostraBrasil")
# checking the city codes from Brazil´s National Statistics Office (IBGE)
library(AmostraBrasil)

#Filter only year 2020
big_df2<-big_df %>% filter(ano==2020)

# changing comma to period to save this later as .R file
big_df$nMx <- gsub("\\,", ".", big_df$nMx)
big_df$nqx <- gsub("\\,", ".", big_df$nqx)
big_df$lx <- gsub("\\,", ".", big_df$lx)
big_df$ndx <- gsub("\\,", ".", big_df$ndx)
big_df$nLx <- gsub("\\,", ".", big_df$nLx)
big_df$Tx <- gsub("\\,", ".", big_df$Tx)
big_df$ex <- gsub("\\,", ".", big_df$ex)


big_df$nMx <-as.numeric(big_df$nMx)
big_df$nqx <-as.numeric(big_df$nqx)
big_df$lx <-as.numeric(big_df$lx)
big_df$ndx <-as.numeric(big_df$ndx)
big_df$nLx <-as.numeric(big_df$nLx)
big_df$Tx <-as.numeric(big_df$Tx)
big_df$ex <-as.numeric(big_df$ex)

# changing comma to period to perform estimations for the 2020 subset data
big_df2$nMx <- gsub("\\,", ".", big_df2$nMx)
big_df2$nqx <- gsub("\\,", ".", big_df2$nqx)
big_df2$lx <- gsub("\\,", ".", big_df2$lx)
big_df2$ndx <- gsub("\\,", ".", big_df2$ndx)
big_df2$nLx <- gsub("\\,", ".", big_df2$nLx)
big_df2$Tx <- gsub("\\,", ".", big_df2$Tx)
big_df2$ex <- gsub("\\,", ".", big_df2$ex)


big_df2$nMx <-as.numeric(big_df2$nMx)
big_df2$nqx <-as.numeric(big_df2$nqx)
big_df2$lx <-as.numeric(big_df2$lx)
big_df2$ndx <-as.numeric(big_df2$ndx)
big_df2$nLx <-as.numeric(big_df2$nLx)
big_df2$Tx <-as.numeric(big_df2$Tx)
big_df2$ex <-as.numeric(big_df2$ex)

# save as single R file
head(big_df)
big_df<-big_df[,-1]
saveRDS(big_df, file = "lt_bra_mun.rds")

#devtools::install_github("jimhester/archive")
#library(archive)
#saveCRDS <- function(object, filename, filter=NULL) {
#  stopifnot(filter %in% c('zstd', 'lz4'))
#  con = archive::file_write(file = filename, filter=filter)
#  open(con)
#  saveRDS(object, con)
#  close(con)
#}


# ok, now estimating nmx values for total pop
# Miguel´s suggestion
# m(x)=(m_f(x)*l_f(x)+ m_m(x)*l_m(x)) /(l_f(x)+l_m(x))
# first turn everything to wide it will be easier

big_df2_wide<-big_df2 %>%
  pivot_wider(names_from = sexo, values_from = 7:13 )

big_df2_wide_total<-big_df2_wide %>%
  group_by(UF, `Codigo Municipio`, `Nome Municipio`, idade) %>%
  mutate(nmx_total=(((nMx_f*lx_f)+(nMx_m*lx_m))/(lx_f+lx_m))) %>%
  select(1:5,20)


# if going back to long and keeping
#big_df2_total<-big_df2_wide_total %>%
# pivot_longer(cols = -c(1:6),
#               names_to = c(".value", "num"), names_sep = "_")


# merge everything


pop_single<-pop_city_grad %>%
  filter(Age<100) %>%
  mutate(code_city=floor(Armenor/10))


colnames(pop_single)<-c("code","city","Age","Population","code_2")
colnames(big_df2_wide_total)<-c("UF","code_2","city","Age","year","nmx")
big_df2_wide_total <- big_df2_wide_total[,-(5)]
big_df2_wide_total$code_2<-as.numeric(big_df2_wide_total$code_2)
big_df2_wide_total$Age<-as.numeric(big_df2_wide_total$Age)
big_df2_wide_total$nmx<-as.numeric(big_df2_wide_total$nmx)

pop_single$code_2<-as.numeric(pop_single$code_2)
pop_single$Age<-as.numeric(pop_single$Age)
pop_single$Population<-as.numeric(pop_single$Population)

col_order <- c("city", "UF", "Age", "nmx", "code_2")
big_df2_wide_total <- big_df2_wide_total[, col_order]

big_df2_wide_total<-as.data.frame(big_df2_wide_total)

# merging

nmx_pop_city<-inner_join(pop_single,big_df2_wide_total, by=c("Age", "code_2"))

nmx_pop_city$city.y<-ifelse(nmx_pop_city$city.y=="Distrito Federal","Brasília",nmx_pop_city$city.y )
# here there are the 500 places for which there is no data for mortality. check this with Bernardo
na_merge <- nmx_pop_city[!complete.cases( nmx_pop_city), ]



# now combining with the CFRs

seroprevalence <- read_excel("seroprevalence.xls")

#changing to lower case

seroprevalence$City<- lapply(seroprevalence$City, tolower)

# adding only first letter as upper case
seroprevalence$City<- str_to_title(seroprevalence$City)

nmx_pop_city$city.y<- str_to_title(nmx_pop_city$city.y)
nmx_pop_city$city.y<-as.character(nmx_pop_city$city.y)

nmx_pop_city_sum<-nmx_pop_city %>%
  group_by(code,code_2,city.y,city.x, UF) %>%
  dplyr::summarise(sum(Population))


# need to join by city name and UF to guarantee, since there are cities with the same names,
# e.g Belém, which has 3 different UFs.
ser_nmx_pop<-left_join(seroprevalence,nmx_pop_city_sum, by=c("City"="city.y", "UF"="UF")) #oops
# in the life tables the name of the municipio is Distrito Federal, while in the pop file it is
# Brasília..

nmx_pop_city_sum$city.y<-ifelse(nmx_pop_city_sum$city.y=="Distrito Federal","Brasília",nmx_pop_city_sum$city.y )

# going again..

ser_nmx_pop<-left_join(seroprevalence,nmx_pop_city_sum, by=c("City"="city.y", "UF"="UF"))

# cfrs

cfr_br <- fread("cases-brazil-cities.csv", encoding="UTF-8")
cfr_br <-cfr_br  %>%
  separate(city, c("City", "UF"), sep = "[/]")
View(cfr_br)

cfr_br$ibgeID<-as.numeric(cfr_br$ibgeID)

cfr_nmx<-inner_join(ser_nmx_pop,cfr_br, by=c("code"="ibgeID"))

# now adding dummy for city capital
capitals_br <- read_excel("capitals_br.xlsx")
capitals_br$City<- str_to_title(capitals_br$City)
cfr_nmx$Capital<-ifelse(cfr_nmx$City.x%in%capitals_br$City,1,0)

# checking
cfr_nmx_capitals<-cfr_nmx %>% filter(Capital==1)  # ok everything

cfr_nmx_sero<-cfr_nmx %>%
  select(c(1:7,14:23))

colnames(cfr_nmx_sero)<-c("UF","City","Sample", "Positive", "Seroprevalence", "IBGEcode","code_2",
                          "deaths" , "totalCases" ,"deaths_per_100k_inhabitants","totalCases_per_100k_inhabitants",
                          "deaths_by_totalCases" ,"_source" , "date" ,"newCases", "newDeaths","Capital")

saveRDS(cfr_nmx_sero, file = "cfr_sero_bra.rds")


View(tot)
names(cfr_nmx_sero)
names(nmx_pop_city)

nmx_pop_city_mer<-nmx_pop_city %>%
  select(c(1,3,4,5,8))

tot<-inner_join(cfr_nmx_sero,nmx_pop_city_mer, by=c("code_2"))
View(tot)

saveRDS(tot, file = "cfr_sero_pop_nmx_bra.rds")


#saving only pop and mortality file, all cities

pop_city_mig<-nmx_pop_city %>%
  select(c(1,3:8))
pop_city_mig<-pop_city_mig[c(1,4,6,5,2,3,7)]

colnames(pop_city_mig)<-c("IBGEcode", "code_2", "UF", "City","Age","Population","nmx")

saveRDS(pop_city_mig, file = "pop_nmx_bra.rds")
