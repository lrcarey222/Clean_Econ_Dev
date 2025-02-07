#GDP R Script

# State Variable - Set this to the abbreviation of the state you want to analyze
state_abbreviation <- "MT"  # Replace with any US state abbreviation
state_name <- "Montana"  # Replace with the full name of any US state

#Set the Working Directory to your Username and update output folder for saved charts etc
setwd("C:/Users/LCarey.RMI/")
output_folder <- paste0("OneDrive - RMI/Documents - US Program/6_Projects/Clean Regional Economic Development/ACRE/Slide Decks/States/",state_abbreviation)

#GDP by Industry----------------------------
url <- "https://apps.bea.gov/regional/zip/SAGDP.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

gdp_ind <- read.csv(files[grepl("SAGDP9N__ALL_AREAS_1997_2023.csv", files)], stringsAsFactors = FALSE)

#Make relevant columns numeric and add GDP growth variable
years <- 1997:2023
year_cols <- paste0("X", years)
gdp_ind <- gdp_ind %>%
  mutate(across(all_of(year_cols), ~ as.numeric(gsub(",", "", .)))) %>%
  mutate(gdp_growth_1722 = (X2022 - X2017) / X2017 * 100,
         gdp_growth_1823 = (X2023 - X2018) / X2018 * 100,
         gdp_growth_2322=(X2023-X2022)/X2022*100) 

#Filter for 'All industry total'
gdp_state_total<-gdp_ind %>%
  filter(Description=="All industry total ")%>%
  mutate(fips=as.numeric(GeoFIPS)) %>%
  filter(fips<60000)

#GDP Growth Map
us_states<-usmap::us_map(regions = "states")
state_map_data <- left_join(us_states, gdp_state_total, by = c("full" = "GeoName"))

state_labels<-centroid_labels(regions = c("states"))
state_labels <- state_labels %>%
  left_join(gdp_state_total,by=c("full"="GeoName"))

gdp_state_total_map<-ggplot() +
  geom_polygon(data = state_map_data, aes(x=x,y=y,group=group, fill = gdp_growth_1823), color = "white") +
  geom_text(data = state_labels, aes(x = x, y = y, label = abbr), size = 2, color = "black", fontface = "bold") +
  scale_fill_gradient2(low="#F8931D",mid="white",high="#0989B1", midpoint=mean(gdp_state_total$gdp_growth_1823), na.value = "grey90", name = "% Growth") +
  labs(title = "Economic Growth by State", 
       subtitle = "Percentage Growth, 2018-2023",
       fill = "% Growth",
       caption="Source: Bureau of Economic Analysis") +
  theme(legend.position=c(0.9,0.1))+
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = "white"),  # Sets plot background to white
        panel.background = element_rect(fill = "white", color = "white"))  # Sets panel background to white
ggsave(file.path(output_folder, paste0(state_abbreviation,"_gdp_state_total_map", ".png")), 
       plot = gdp_state_total_map,width=8,height=6,units="in",dpi=300)


#Regional GDP Growth Chart
region_abbrv<-states_simple %>%
  filter(abbr == state_abbreviation) 

region_division <- census_divisions %>%
  filter(State.Code ==state_abbreviation)

state_gdp_index <- gdp_state_total %>%
  right_join(states_simple,by=c("GeoName"="full")) %>%
  left_join(census_divisions,by=c("GeoName"="State")) %>%
  pivot_longer(cols = X1997:X2023, names_to = "Year", values_to = "GDP") %>%
  mutate(Year = as.numeric(str_remove(Year, "X"))) %>%
  group_by(GeoName) %>%
  mutate(gdp_index_2013 = 100*GDP/GDP[Year=="2012"])%>%
  filter(Division==region_division$Division,
         State.Code != "DC",
         Year>2011) %>%
  ungroup() %>%
  select(abbr,Year,gdp_index_2013) %>%
  pivot_wider(names_from = abbr, values_from = gdp_index_2013) %>%
  write.csv(paste0(output_folder,"/",state_abbreviation,"_gdp_index.csv")) 
  

#Filter to 2 or 3-digit NAICS Level
gdp_ind2 <- gdp_ind %>%
  filter(str_detect(IndustryClassification, "^\\d{2}$")|IndustryClassification=="31-33")%>%
  filter(IndustryClassification!="92")


gdp_ind3<-gdp_ind %>%
  filter(!IndustryClassification %in% c("...","11","21","31-33","311-316,322-326",
                                        "321,327-339","48-49","54,55,56",
                                        "61,62","71,72","51","52","53","56","62","71","72"),
         LineCode<83) 


#National Clean Energy Economy
gdp_energyeconomy<-gdp_ind3 %>% 
  filter(is.na(Region)) %>% 
  mutate(X2023=as.numeric(X2023)) %>%
  mutate(energyeconomy=ifelse(IndustryClassification %in% c("212","22","23","321","327","331","332","333","334","335","3361-3363","325","481","484"),"Clean Energy Economy","Other"))
gdp_energyeconomy_total<-gdp_energyeconomy %>%
  group_by(energyeconomy) %>%
  summarize_at(vars(X2023),sum,na.rm=T)%>%
  mutate(share=X2023/sum(X2023)*100)

#Treemap
output_file <- file.path(output_folder, paste0("_gdp3_treemap.png"))
png(filename = output_file, width = 8, height = 8, units = 'in', res = 300)

treemap(
  gdp_energyeconomy,
  index = c("energyeconomy", "Description"),
  vSize = "X2023",
  vColor="label",
  palette = rmi_palette,
  title = paste0("US Economy"),
  #caption = "Source: Bureau of Economic Analysis",
  fontsize.title = 16,
  fontsize.labels = 12,
  align.labels = list(
    c("left", "top"),
    c("center", "center"),
    fontface.labels = list(
      2,  # Bold text for better visibility
      1
    ),
    
    fontsize.labels = list(
      12,  # Bold text for better visibility
      10
    )
  )
)

# Close the device
dev.off()


#2-Digit NAICS: Calculate State and Local Shares for LQ Calculations
total_gdp_by_year <- gdp_ind2 %>%
  mutate(across(all_of(year_cols), ~ as.numeric(gsub(",", "", .)))) %>%
  group_by(GeoName) %>%
  summarize_at(vars(X2023),sum,na.rm=T) 

gdp_proportions <- gdp_ind2 %>%
  left_join(total_gdp_by_year %>% select(GeoName,X2023), by = "GeoName", suffix = c("", "_total")) %>%
  select(GeoName,Description, X2023,X2023_total) %>%
  mutate(gdp_share=X2023/X2023_total)

# National-level proportions
national_proportions <- gdp_proportions %>%
  filter(GeoName == "United States") %>%
  select(Description, X2023, gdp_share_national = gdp_share)
# State-level data, exclude national totals
state_proportions <- gdp_proportions %>%
  filter(GeoName != "United States")

#Location Quotients
location_quotients <- state_proportions %>%
  left_join(national_proportions, by = "Description") %>%
  mutate(LQ = gdp_share / gdp_share_national,
         weighted_LQ = LQ * gdp_share)  # Weighting by regional share

# Compute the Hachman index
hachman_indices <- location_quotients %>%
  inner_join(states_simple,by=c("GeoName"="full")) %>%
  mutate(sqrt_product = sqrt(gdp_share * gdp_share_national)) %>%
  group_by(GeoName) %>%
  summarize(HI = 100*sum(sqrt_product,na.rm=T)^2, .groups = 'drop') %>% # Reciprocal of the sum of weighted LQs scaled by 100
  mutate(HI_bin = cut(HI, breaks = c(0, 80,90,92,94,96,98, 100), labels = c("Very Low (0-80)","Low (80-90)", "Medium (90-92)","Medium-High (92-94)", "High (94-96)", "Very High (96-98)","Highest (98-100)")))

# Plotting the US state map with Hachman Index
state_map_data <- left_join(us_states, hachman_indices, by = c("full" = "GeoName"))

state_labels<-centroid_labels(regions = c("states"))
state_labels <- state_labels %>%
  left_join(hachman_indices,by=c("full"="GeoName"))

hachman_map<-ggplot() +
  geom_polygon(data = state_map_data, aes(x=x,y=y,group=group,fill = HI_bin), color = "white") +
  geom_text(data = state_labels, aes(x = x, y = y, label = abbr), size = 2, color = "black", fontface = "bold") +
  scale_fill_manual(values = rev(rmi_palette), na.value = "grey90", name = "Investment Share Relative to National Average") +
  labs(title = "Economic Diversity by State", 
       subtitle = "A Hachman Index score ranges from 0 to 100. A higher score indicates that the subject area's industrial distribution more closely resembles that of the US as a whole, and is therefore diverse.",
       fill = "Hachman Index",
       caption="Source: Bureau of Economic Analysis") +
  theme(legend.position=c(0.9,0.1))+
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = "white"),  # Sets plot background to white
        panel.background = element_rect(fill = "white", color = "white"))  # Sets panel background to white


ggsave(file.path(output_folder, paste0(state_abbreviation,"_hachman_map", ".png")), 
       plot = hachman_map,width=8,height=6,units="in",dpi=300)

#State Location Quotient
state_lq<-location_quotients %>%
  filter(GeoName==state_name) %>%
  select(Description,LQ) %>%
  arrange(desc(LQ))%>%
  left_join(gdp_ind %>% 
              filter(GeoName==state_name) %>% 
              select(GeoName,Description,X2023,gdp_growth_1823),by=c("Description"="Description")) %>%
  mutate(label=case_when(
    gdp_growth_1823>weighted.mean(gdp_growth_1823,w=X2023) & LQ >1 ~ "High Growth/High Specialization",
    gdp_growth_1823<weighted.mean(gdp_growth_1823,w=X2023)  & LQ >1 ~ "Low Growth/High Specialization",
    gdp_growth_1823>weighted.mean(gdp_growth_1823,w=X2023)  & LQ <1 ~ "High Growth/Low Specialization",
    gdp_growth_1823<weighted.mean(gdp_growth_1823,w=X2023) & LQ <1 ~ "Low Growth/Low Specialization"))


#Chart: State Location Quotient
state_lq_plot<-ggplot(data= ,aes(x=LQ,y=gdp_growth_1823,size=X2023,color=label)) +
  geom_point(aes(fill=label),shape=21,color="black",stroke=0.5,alpha=0.75) +
  geom_text_repel(aes(label = ifelse(LQ>1,paste(Description,"= ", round(gdp_growth_1823,0),"% growth."),"")), 
                  box.padding = 0.5, 
                  point.padding = 0.3, 
                  segment.color = 'grey',
                  size=2,
                  color='black') +
  labs(title=paste("Growth and Specialization in ",state_name), 
       y="GDP Growth (18-23)", x="Location Quotient",
       caption="Source: BEA") +
  geom_vline(xintercept = 1,color='darkgrey') +
  geom_hline(yintercept= weighted.mean(state_lq$gdp_growth_1823,w=state_lq$X2023) ,color='darkgrey') +
  theme_classic()+
  scale_fill_manual(values = rmi_palette)+
  scale_size(range = c(3, 20)) +  # Controlling the size of the bubbles
  theme(legend.position="none")

ggsave(file.path(output_folder, paste0(state_abbreviation,"_state_lq_plot", ".png")), 
       plot = state_lq_plot,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)


#3-Digit NAICS: Calculate State and Local Shares for LQ Calculations
total_gdp_by_year_3 <- gdp_ind3 %>%
  mutate(across(all_of(year_cols), ~ as.numeric(gsub(",", "", .)))) %>%
  group_by(GeoName) %>%
  summarize_at(vars(X2022),sum,na.rm=T) 

gdp_proportions_3 <- gdp_ind3 %>%
  left_join(total_gdp_by_year_3 %>% select(GeoName,X2022), by = "GeoName", suffix = c("", "_total")) %>%
  select(GeoName,Description, X2022,X2022_total) %>%
  mutate(gdp_share=X2022/X2022_total)

# National-level proportions
national_proportions_3 <- gdp_proportions_3 %>%
  filter(GeoName == "United States") %>%
  select(Description, X2022, gdp_share_national = gdp_share)
# State-level data, exclude national totals
state_proportions_3 <- gdp_proportions_3 %>%
  filter(GeoName != "United States")

#Location Quotients
location_quotients_3 <- state_proportions_3 %>%
  left_join(national_proportions_3, by = "Description") %>%
  mutate(LQ = gdp_share / gdp_share_national,
         weighted_LQ = LQ * gdp_share)  # Weighting by regional share

state_lq_3<-location_quotients_3 %>%
  filter(GeoName==state_name) %>%
  select(Description,LQ) %>%
  arrange(desc(LQ))%>%
  left_join(gdp_ind %>% 
              filter(GeoName==state_name) %>% 
              select(GeoName,Description,X2022,gdp_growth_1722),by=c("Description"="Description")) %>%
  mutate(label=case_when(
    gdp_growth_1722>weighted.mean(gdp_growth_1722,w=X2022) & LQ >1 ~ "High Growth/High Specialization",
    gdp_growth_1722<weighted.mean(gdp_growth_1722,w=X2022)  & LQ >1 ~ "Low Growth/High Specialization",
    gdp_growth_1722>weighted.mean(gdp_growth_1722,w=X2022)  & LQ <1 ~ "High Growth/Low Specialization",
    gdp_growth_1722<weighted.mean(gdp_growth_1722,w=X2022) & LQ <1 ~ "Low Growth/Low Specialization"))

state_quadrant <- state_lq_3 %>%
  group_by(label) %>%
  summarize_at(vars(X2022),sum,na.rm=T) %>%
  mutate(share=X2022/sum(X2022))

#Treemap
library(treemap)

# Specify the output path and filename
output_file <- file.path(output_folder, paste0(state_abbreviation, "_gdp_treemap.png"))

# Open a PNG device
png(filename = output_file, width = 8, height = 8, units = 'in', res = 300)

# Create the treemap
treemap(
  state_lq_3,
  index = c("label", "Description"),
  vSize = "X2022",
  vColor="label",
  palette = rmi_palette,
  title = paste0(state_name,"'s Economy by Industry Growth and Specialization"),
  #caption = "Source: Bureau of Economic Analysis",
  fontsize.title = 16,
  fontsize.labels = 12,
  align.labels = list(
    c("left", "top"),
    c("center", "center"),
    fontface.labels = list(
      2,  # Bold text for better visibility
      1
    ),
    
    fontsize.labels = list(
      12,  # Bold text for better visibility
      10
    )
  )
)

# Close the device
dev.off()


#For Clean Energy Industries
clean_gdpind<-gdp_ind %>%
  mutate(IndustryClassification_3=substr(IndustryClassification,1,3),
         IndustryClassification_2=substr(IndustryClassification,1,2)) %>%
  left_join(eti_long %>% 
              select(`3-Digit Code`,Sector, Subsector,Technology) %>%
              mutate(naics3=as.character(`3-Digit Code`)),by=c("IndustryClassification_3"="naics3")) %>%
  left_join(eti_long %>% 
              select(`2-Digit Code`,Sector,Subsector,Technology) %>%
              mutate(naics2=as.character(`2-Digit Code`)),by=c("IndustryClassification_2"="naics2")) %>%
  mutate(sector=ifelse(is.na(Sector.x),Sector.y,Sector.x)) %>%
  mutate(subsector=ifelse(is.na(Subsector.x),Subsector.y,Subsector.x)) %>%
  mutate(technology=ifelse(is.na(Technology.x),Technology.y,Technology.x)) %>%
  filter(!is.na(sector),
         sector != "Transition Enabling Sector") %>%
  distinct(GeoName,IndustryClassification,Description,X2017,X2018,X2021,X2022,X2023,gdp_growth_1722,gdp_growth_1823,sector,subsector,technology) %>%
  mutate(gdp_2122=1-X2022/X2021)


state_cleangdp<-clean_gdpind %>%
  filter(GeoName==state_name) 
  

state_lq_clean <-state_lq %>%
  filter(Description %in% clean_gdpind$Description) 
state_lq_dirty<- state_lq %>%
  filter(!(Description %in% clean_gdpind$Description))

state_lq_clean_plot<-ggplot() +
  geom_point(data=state_lq_dirty,aes(x=LQ,y=gdp_growth_1823,size=X2023),color='grey',alpha=0.5) +
  geom_point(data=state_lq_clean,aes(x=LQ,y=gdp_growth_1823,size=X2023,fill=label),shape=21,color="black",stroke=0.5,alpha=0.75) +
  geom_text_repel(data=state_lq_clean,aes(x=LQ,y=gdp_growth_1823,label = paste(Description,"= ", round(gdp_growth_1823,0),"% growth.")), 
                  box.padding = 0.2, 
                  point.padding = 0.2, 
                  segment.color = 'grey',
                  size=3.5,
                  color='black') +
  labs(title=paste("Growth and Specialization in ",state_name," in clean energy-related industries."),
       subtitle="Clean energy-related industries are defined as those with the potential to participate in the energy transition technology supply chain.",
       y="GDP Growth (17-22)", x="Location Quotient",
       caption="Source: BEA, Clean Growth Tool") +
  geom_vline(xintercept = 1,color='darkgrey') +
  geom_hline(yintercept= weighted.mean(state_lq$gdp_growth_1823,w=state_lq$X2023) ,color='darkgrey') +
  theme_classic()+
  scale_fill_manual(values = rmi_palette)+
  scale_size(range = c(3, 20)) +  # Controlling the size of the bubbles
  theme(legend.position="none")

ggsave(file.path(output_folder, paste0(state_abbreviation,"_state_lq_clean_plot", ".png")), 
       plot = state_lq_clean_plot,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)


#State 3-Digit Clean Energy Industries

state_lq_clean_3 <-state_lq_3 %>%
  inner_join(clean_gdpind %>% select(GeoName,Description,sector,subsector,X2021,gdp_2122),by=c("GeoName","Description")) %>%
  mutate(gdp_contr=X2022-X2021) %>%
  distinct(GeoName,Description,X2022,gdp_2122,gdp_growth_1722,LQ,gdp_contr,label)
  


state_lq_clean3_plot<-ggplot() +
  geom_point(data=state_lq_clean_3,aes(x=LQ,y=gdp_growth_1722,size=X2022,fill=label),shape=21,color="black",stroke=0.5,alpha=0.75) +
  geom_text_repel(data=state_lq_clean_3 %>% filter(LQ>1),aes(x=LQ,y=gdp_growth_1722,label = Description), 
                  box.padding = 0.2, 
                  point.padding = 0.2, 
                  segment.color = 'grey',
                  size=3.5,
                  color='black') +
  labs(title=paste("Growth and Specialization in ",state_name," in clean energy-related industries."),
       subtitle="Clean energy-related industries are defined as those with the potential to participate in the energy transition technology supply chain.",
       y="GDP Growth (17-22)", x="Location Quotient",
       caption="Source: BEA, Clean Growth Tool") +
  geom_vline(xintercept = 1,color='darkgrey') +
  geom_hline(yintercept= weighted.mean(state_lq$gdp_growth_1722,w=state_lq$X2022) ,color='darkgrey') +
  theme_classic()+
  scale_fill_manual(values = rmi_palette)+
  scale_size(range = c(3, 20)) +  # Controlling the size of the bubbles
  theme(legend.position="none")


ggsave(file.path(output_folder, paste0(state_abbreviation,"_state_lq_clean3_plot", ".png")), 
       plot = state_lq_clean3_plot,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)

# Specify the output path and filename
output_file <- file.path(output_folder, paste0(state_abbreviation, "_gdp3_treemap.png"))

# Open a PNG device
png(filename = output_file, width = 8, height = 8, units = 'in', res = 300)

# Create the treemap
treemap(
  state_lq_clean_3,
  index = c("label", "Description"),
  vSize = "X2022",
  vColor="label",
  palette = rmi_palette,
  title = paste0(state_name,"'s Clean Energy Economy by Industry Growth and Specialization"),
  #caption = "Source: Bureau of Economic Analysis",
  fontsize.title = 16,
  fontsize.labels = 12,
  align.labels = list(
    c("left", "top"),
    c("center", "center"),
    fontface.labels = list(
      2,  # Bold text for better visibility
      1
    ),
    
    fontsize.labels = list(
      12,  # Bold text for better visibility
      10
    )
  )
)

# Close the device
dev.off()

#Growth Chart
clean_gdpgrowth <- ggplot(data=state_lq_clean_3,
                          aes(x=reorder(Description,gdp_contr),y=gdp_contr))+
  geom_bar(stat="identity",fill=rmi_palette[1],color='black')+
  coord_flip()+
  labs(title = paste("2021-2022 GDP Contribution from ", state_name,"'s Clean Energy Industries"),
       x = "Industry",
       y = "GDP Contribution ($m)",
       subtitle = "Change in GDP by industry 2021-2022") +
  theme_minimal()


ggsave(file.path(output_folder, paste0(state_abbreviation,"_state_clean_gdp_contr_plot", ".png")), 
       plot = clean_gdpgrowth,
       width = 8,   # Width of the plot in inches
       height = 8,   # Height of the plot in inches
       dpi = 300)


#Clean Location Quotients
gdp_proportions_clean <- clean_gdpind %>%
  group_by(GeoName,sector) %>%
  summarize_at(vars(X2023),sum,na.rm=T) %>%
  left_join(total_gdp_by_year %>% select(GeoName,X2023), by = "GeoName", suffix = c("", "_total")) %>%
  mutate(gdp_share=X2023/X2023_total)

# National-level proportions
national_proportions <- gdp_proportions_clean %>%
  filter(GeoName == "United States") %>%
  select(sector, X2023, gdp_share_national = gdp_share)
# State-level data, exclude national totals
state_proportions <- gdp_proportions_clean %>%
  filter(GeoName != "United States")

#Location Quotients
location_quotients <- state_proportions %>%
  left_join(national_proportions, by = "sector") %>%
  mutate(LQ = gdp_share / gdp_share_national,
         weighted_LQ = LQ * gdp_share)  # Weighting by regional share

# Prepare the data: Filter for top 5 sectors by LQ for each GeoName
top_sectors <- location_quotients %>%
  left_join(states_simple,by=c("GeoName.x"="full")) %>%
  filter(region %in% region_abbrv$region) %>%
  group_by(region,GeoName.x,sector) %>%
  summarize_at(vars(LQ), mean, na.rm = TRUE) %>%
  ungroup() %>%
  mutate(state=ifelse(GeoName.x==state_name,1,0)) %>%
  arrange(desc(LQ)) 

# Create the plot
top_sectors_plot<-ggplot(top_sectors, aes(x = reorder(sector, LQ), y = LQ, fill = (GeoName.x))) +
  geom_bar(stat = "identity", position = position_dodge(),
           aes(alpha = ifelse(state == "0", 0.4, 1))) +
  coord_flip() +  # Flip coordinates to make horizontal bars
  scale_fill_manual(values = expanded_palette, 
                    name = paste(str_to_sentence(top_sectors$region), " States")) +
  scale_alpha_identity() +  # Ensure alpha is interpreted as given
  labs(title = paste("Transition Sector Specializations in ", state_name),
       x = "Transition Sector",
       y = "Location Quotient (LQ)",
       subtitle = "Average Location Quotient in Transition Sector Categories") +
  theme_minimal() +
  geom_hline(yintercept=1,color='darkgrey')+
  geom_text(aes(x = 0, y = 1), 
            label = "National Average", hjust = 0.5, vjust = -0.25)+
  theme(legend.position = c(0.8,0.25),
        plot.title = element_text(size = 14, face = "bold"),
        plot.subtitle = element_text(size = 12))



#National Clean Energy Industry Comparison
national_clean_gdp<-clean_gdpind %>%
  filter(!is.na(sector),is.na(gdp_growth_1823)) %>%
  group_by(GeoName) %>%
  summarize_at(vars(X2017,X2021,X2022),sum,na.rm=T) %>%
  mutate(gdp_1722=1-X2022/X2017,
         gdp_2122=1-X2022/X2021) %>%
  arrange(desc(gdp_2122))


#Energy-Related GDP Growth Map
us_states<-usmap::us_map(regions = "states")
state_map_data <- left_join(us_states, national_clean_gdp, by = c("full" = "GeoName"))

state_labels<-centroid_labels(regions = c("states"))
state_labels <- state_labels %>%
  left_join(gdp_state_total,by=c("full"="GeoName"))

gdp_state_clean_map<-ggplot() +
  geom_polygon(data = state_map_data, aes(x=x,y=y,group=group, fill = gdp_2122), color = "white") +
  geom_text(data = state_labels, aes(x = x, y = y, label = abbr), size = 2, color = "black", fontface = "bold") +
  scale_fill_gradient2(low="#F8931D",mid="white",high="#0989B1", midpoint=0, na.value = "grey90", name = "% Growth") +
  labs(title = "Economic Growth in Clean Energy-Related Industries, by State", 
       subtitle = "Percentage Growth, 2021-2022",
       fill = "% Growth",
       caption="Source: Bureau of Economic Analysis") +
  theme(legend.position=c(0.9,0.1))+
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = "white"),  # Sets plot background to white
        panel.background = element_rect(fill = "white", color = "white"))  # Sets panel background to white
ggsave(file.path(output_folder, paste0(state_abbreviation,"_gdp_state_clean_map", ".png")), 
       plot = gdp_state_clean_map,width=8,height=6,units="in",dpi=300)


#State Population
library(tidycensus)
census_api_key('0b3d37ac56ab19c5a65cbc188f82d8ce5b36cfe6',install=T)

years <- 2012:2023

# Initialize an empty list to store data
statepop_list <- list()

# Loop over each year
for (yr in years) {
  statepop_year <- get_acs(geography = "state", 
                           variable = "B01001_001",
                           year = yr,
                           survey = "acs5",
                           geometry = FALSE)
  statepop_year$year <- yr  # Add a year column to each dataset
  statepop_list[[as.character(yr)]] <- statepop_year
}

# Combine all years' data into one data frame
statepop <- bind_rows(statepop_list)


region_division <- census_divisions %>%
  filter(State.Code ==state_abbreviation)

statepop_region<-statepop %>%
  left_join(states_simple,by=c("NAME"="full")) %>%
  left_join(census_divisions,by=c("NAME"="State")) %>%
  filter(Division==region_division$Division,
         State.Code!="DC") %>%
  rename(pop=estimate) %>%
  mutate(pop=as.numeric(pop)) %>%
  group_by(abbr) %>%
  mutate(pop_index=pop/pop[year==2012]*100) %>%
  ungroup() %>%
  select(abbr,year,pop_index) %>%
  pivot_wider(names_from = abbr, values_from = pop_index) %>%
  write.csv(paste0(output_folder,"/",state_abbreviation,"_pop_index.csv"))
  

#County Population
county_pop <- county_pop %>%
  mutate(fips = as.numeric(sprintf("%02d%03d", STATE, COUNTY))) %>%
  left_join(EAs, by=c("fips"="fips")) 

region_pop <- county_pop %>%
  left_join(states_simple, by = c("STNAME" = "full")) %>%
  left_join(census_divisions, by = c("abbr" = "State.Code")) %>%
  mutate(`EA Name` = ifelse(FIPS %in% great_falls$FIPS, "Great Falls, MT", `EA Name`)) %>%
  filter(
    Division == region_division$Division,
         STNAME != "District of Columbia",
         !is.na(`EA Name`)) %>%
  group_by(`EA Name`) %>%
  summarize(across(c(POPESTIMATE2023, NATURALCHG2020:NATURALCHG2023, NETMIG2020:NETMIG2023), sum, na.rm = TRUE)) %>%
  ungroup() %>%
  rowwise() %>%
  mutate(natural = sum(c_across(NATURALCHG2020:NATURALCHG2023))/POPESTIMATE2023*100,
         netmig = sum(c_across(NETMIG2020:NETMIG2023))/POPESTIMATE2023*100,
         total = natural + netmig) %>%
  select(`EA Name`, natural, netmig, total) %>%
  arrange(desc(total))
write.csv(region_pop, paste0(output_folder, "/", state_abbreviation, "_region_pop.csv"))



#County GDP by Industry

#GDP by Industry----------------------------
url <- "https://apps.bea.gov/regional/zip/CAGDP11.zip"
temp_zip <- tempfile(fileext = ".zip")
download(url, temp_zip, mode = "wb")
temp_dir <- tempdir()
unzip(temp_zip, exdir = temp_dir)
files <- list.files(temp_dir, full.names = TRUE)

county_gdp_ind<-read.csv(files[grepl("CAGDP11__ALL_AREAS_2002_2023.csv", files)], stringsAsFactors = FALSE)