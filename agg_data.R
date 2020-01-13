# Code to create aggregate data cars.fst
# 
# Not well documented. Look at the code itself
# 
# Assumes we have original data in fst
# format for each year.
#
# Run the code in run()
run = function() {

  
  year = 2017
  years = 2012:2018

  for (year in years) {
    agg_year_data(year)
  }
  
  li = lapply(years, function(year) {
    agg.file = paste0("cars_agg_", year, ".fst")
    fst::read.fst(agg.file)    
  })
  dat = dplyr::bind_rows(li)
  fst::write.fst(dat, "cars_agg.fst")
  

  # Improves firm names and saves as cars.fst
  make_firm_names()
  
  # Create groups.csv to match firms with groups
  make.groups()
  
  dat = fst::read_fst("cars.fst")
  if (!"group" %in% colnames(dat)) {
    groups.df = readr::read_csv("groups.csv")
    library(dplyrExtras)
    dat = left_join(dat, groups.df, by="firm") %>%
      mutate_rows(is.na(group), group=firm)
  }
  dat = dat %>%
    select(-mp, -man, -mh, -mms,-red_wltp) %>%
    rename(country=ms, pool=group, reduction_nedc=red_nedc)
  dat = dat %>%
    select(year, pool, firm, country, cn,ft,q, everything())

  dat = fst::write_fst(dat,"cars2.fst", compress = 50)  
}


agg_year_data = function(year) {
  library(dplyr)
  library(forcats)
  agg.file = paste0("cars_agg_", year, ".fst")
  cat("\nAggregate ", year)
  if (file.exists(agg.file)) return()
  
  dat = fst::read_fst(paste0("cars", year,".fst"))
  colnames(dat) = tolower(colnames(dat))
  
  if ("e..g.km." %in% colnames(dat)) {
    dat = rename(dat, co2_km = e..g.km., mass = m..kg., cm3=ec..cm3., power=ep..kw., elec=z..wh.km., qu=r)
  } else if ("enedc..g.km." %in% colnames(dat)) {
    dat = rename(dat, co2_km = enedc..g.km., mass = m..kg., cm3=ec..cm3., power=ep..kw., elec=z..wh.km., qu=r,wltp=NA, red_nedc=NA, red_wltp=NA,)
    
  } else if ("e (g/km)" %in% colnames(dat)) {
    dat = rename(dat, co2_km = `e (g/km)`, mass = `m (kg)`, cm3=`ec (cm3)`, power=`ep (kw)`, elec=`z (wh/km)`, qu=r, wltp=NA, red_nedc=NA, red_wltp=NA)
    
  } else if ("enedc (g/km)" %in% colnames(dat)) {
    dat = rename(dat, co2_km = `enedc (g/km)`, mass = `m (kg)`, cm3=`ec (cm3)`, power=`ep (kw)`, elec=`z (wh/km)`, qu=r, wltp =`ewltp (g/km)`, red_nedc = `ernedc (g/km)`, red_wltp = `erwltp (g/km)`)
    
  }
  
  dat = mutate(dat, ft = trimws(tolower(ft)))
  replace = function(vals, pattern, with) {
    vals[vals==pattern] = with
    vals
  }
  
  dat$ft = replace(dat$ft,"petrol/electric","petrol-electric")
  dat$ft = replace(dat$ft,"petrol phev","petrol-electric")
  dat$ft = replace(dat$ft,"hybrid/petrol/e","petrol-electric")
  dat$ft = replace(dat$ft,"diesel/electric","diesel-electric")
  dat$ft = replace(dat$ft,"","unknown")

  
  #dat = mutate(dat, ft = fct_infreq(ft) %>% fct_rev)
  dat$year = year
  
  
  library(dplyr)
  # Aggregate by commercial vehicle type and fuel type
  d = dat %>%
    group_by(year,ms,mp,man,mh,mms, cn, ft, it ) %>%
    summarize(
      co2_km = weighted.mean(co2_km, qu, na.rm=TRUE),
      mass = weighted.mean(mass, qu, na.rm=TRUE),
      cm3 = weighted.mean(cm3, qu, na.rm=TRUE),
      power = weighted.mean(power, qu, na.rm=TRUE),
      elec = weighted.mean(elec, qu, na.rm=TRUE),
      q = sum(qu, na.rm=TRUE),
      wltp = weighted.mean(wltp, qu, na.rm=TRUE),
      red_nedc = weighted.mean(red_nedc, qu, na.rm=TRUE),
      red_wltp = weighted.mean(red_wltp, qu, na.rm=TRUE)
    ) %>%
    ungroup()
  fst::write_fst(d, agg.file)  
}


make_firm_names = function() {
  library(dplyr)  
  library(dplyrExtras)
  library(stringtools)
  dat = fst::read_fst("cars_agg.fst")
  
  dat = dat %>% mutate_at(c("man","mh","mms"), tolower)
  
  mdat = dat %>%
    group_by( man, mh, mms) %>%
    summarize(q = sum(q, na.rm=TRUE)) %>%
    ungroup %>%
    dplyr::mutate_if(is.character,tolower) %>%
    mutate(row = 1:n()) %>%
    bind_rows(tibble(man="gm",mh="general motors", mms="general motors company"))

  ignore.names = c("aa-iva","aa-nss","aa_iva","aa_nss","duplicate", "unknown","", "out of scope", "n/a","ds","nd","unknwon", "api","amf","amz","bmb","binz","abarth","bautex","capron","carpol","dangel","germaz","humber","pilote","porche","bavaria","laverda","lincoln","maybach","plymouth","amz-kutno","auto-form","mercury","corvette","trans-poz","cita seconda","poclain vehicules","kivi","buick","bierman","binz","mprojekt","dethleffs","tripod","zeszuta","dodge","zimny","automobiles dangel","lancia","chevrolet","smart","lexus","mini","benye","binz & co kg","jlr ltd uk","8","steel","sonst.kfz.hersteller")
  
    ignore.names = c("aa-iva","aa-nss","aa_iva","aa_nss","duplicate", "unknown","", "out of scope", "n/a","ds","nd","unknwon", "api","amf","bmb","abarth","bautex","capron","carpol","dangel","germaz","humber","pilote","porche","bavaria","laverda","lincoln","maybach","plymouth","auto-form","mercury","corvette","trans-poz","cita seconda","poclain vehicules","kivi","buick","bierman","mprojekt","dethleffs","tripod","zeszuta","dodge","zimny","automobiles dangel","lancia","chevrolet","smart","lexus","mini","benye","jlr ltd uk","8","steel","sonst.kfz.hersteller")

  library(tidyr)
  ldat = pivot_longer(select(mdat,-q), c("man", "mh", "mms"))
  
  ndat = ldat %>%
    mutate(value = trimws(gsub("gmbh","",value, fixed=TRUE))) %>%
    filter(!value %in% ignore.names) %>%
    mutate(len = nchar(value)) %>%
    arrange(row, len) %>%
    group_by(row) %>%
    summarize(firm = trimws(first(value))) %>%
    mutate_rows(has.substr(firm,"mercedes"), firm="daimler") %>%
    mutate_rows(has.substr(firm,"bayerische motoren werke"), firm="bmw") %>%
    mutate_rows(has.substr(firm,"vw group pc"), firm="volkswagen") %>%
    mutate_rows(has.substr(firm,"royce"), firm="rolls-royce") %>%
    mutate_rows(has.substr(firm,"chevrolet"), firm="chevrolet") %>%
    mutate_rows(has.substr(firm,"alfa-romeo"), firm="alfa romeo") %>%
    mutate_rows(has.substr(firm,"mitsubishi"), firm="mitsubishi") %>%
#    mutate_rows(has.substr(firm,"shanghaivolksw.(rc)"), firm="volkswagen") %>%
#    mutate_rows(has.substr(firm,"faw-volkswg.(rc)"), firm="volkswagen") %>%
#    mutate_rows(has.substr(firm,"faw-volkswg.(rc)"), firm="volkswagen") %>%
    right_join(mdat, by="row")

  

  library(dplyrExtras)
  ndat = ndat %>% filter(!is.na(firm)) %>%
    mutate(nchar = nchar(firm))%>%
    arrange(nchar)
  

  # ndat = ndat %>% mutate(
  #   ind.firm = 1:n(),
  #   ind.man = match(firm, man, nomatch=1e7L),  
  #   ind.mh = match(firm, mh, nomatch=1e7L),
  #   ind.mms = match(firm, mms, nomatch=1e7L),
  #   first.match = pmin(ind.firm, ind.man, ind.mh, ind.mms)
  # )
  # unique(ndat$firm)
  # ndat$firm = ndat$firm[ndat$first.match]
  # unique(ndat$firm)
  
  ndat$firm = trimws(gsub("gmbh","",ndat$firm, fixed=TRUE))
  firms = unique(ndat$firm)
  firms = firms[order(nchar(firms))]
  firms
  
  ndat$short = ndat$firm

  library(stringtools)
  f = firms[1]
  for (f in rev(firms)) {
    if (f == "man") next
    rows = which(has.substr(ndat$firm, f) & f != ndat$firm)  
    if (length(rows)>0) {
      cat("\n\n",f, " for ", paste0(unique(ndat$firm[rows]), collapse=", "),".")
      ndat$short[rows] = f
      
    }
  }
  
  unique(rev(ndat$short))
  
  res = ndat %>%
    select(firm = firm, short=short, man, mh, mms,q) %>%
#    arrange(nchar(firm), firm) %>%
    group_by(short) %>%
    mutate(firm.q = sum(q, na.rm=TRUE)) %>%
    ungroup() %>%
    arrange(firm.q) %>%  
    distinct()
  res = res %>%
    select(-firm) %>%
    rename(firm=short)

  readr::write_csv(res,"firms.csv")

  fdat = dat %>%
#    select(-firm) %>%
    left_join(select(res, firm, man, mh,mms), by=c("man","mh","mms")) %>%
    select(year, firm, everything()) %>%
    rename(co2 = co2_km)
  
  fst::write_fst(fdat,"cars.fst",compress = 100)
  #readr::write_csv(gdat, "groups.csv")
  #unique(gdat$group)
  
}

make_groups = function() {
  dat = fst::read_fst("cars.fst")
  
  library(dplyrExtras)
  
  
  gdat = dat %>%
    group_by(firm, mp) %>%
    summarize(q = sum(q, na.rm=TRUE)) %>%
    rename(group = mp) %>% 
    mutate(group = tolower(group)) %>%    
    filter(group != "", group != "na") %>%
    mutate_rows(group == "tata motors ltd, jaguar cars ltd , land rover", group="tata motors jaguar land rover") %>%
    mutate_rows(group == "tata motors ltd, jaguar cars ltd, land rover", group="tata motors jaguar land rover") %>%
    mutate_rows(group == "fiat group automobiles spa", group="fca italy spa") %>%
    mutate_rows(group == "toyota-dahaitsu group", group="toyota-mazda") %>%
    mutate_rows(group == "toyota -daihatsu group", group="toyota-mazda") %>%
    arrange(q)
  
  unique(gdat$group)
  # Looking at the data, we see some obvious wrong
  # groups and multiple group names
  
  # We keep for each firm only those groups
  # that have been assigned for the largest number of
  # cars
  
  gdat = gdat %>% 
    group_by(firm) %>%
    arrange(-q) %>%
    slice(1)
  
  unique(gdat$firm)
  
  gdat  = gdat %>% 
    select(-q) %>%
    filter(firm != "opel")
  unique(gdat$group)
  
  # Manual adaptions
  extra.dat = read.csv(textConnection(
    "firm, group
fca,fca italy spa
opel,psa-opel
"))
  
  gdat = gdat %>%
    bind_rows(extra.dat)
  
  readr::write_csv(gdat,"groups.csv")

  # Let us check if there are large firms without groups
  # They probably make their own group
  # But we do not add those groups here
  fdat = dat %>%
    group_by(firm) %>%
    summarize(q = sum(q, na.rm=TRUE)) %>%
    left_join(gdat, by="firm") %>%
    arrange(-q)
  
  d = filter(fdat, is.na(group))
  
}


