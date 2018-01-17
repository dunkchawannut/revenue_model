#Model for marketing performance prediction
options(java.parameters = "-Xm40000m")

rm(list = ls())
gc()

#rm(list = ls())
#install.packages("XLConnect")
#install.packages("purrr")
#install.packages("caret" , dependencies =  TRUE) #FOR CARET NEED TO UPGRADE R VERSION TO MATCH THE PACKAGE
library("RJDBC") ; library("lubridate") ; library("dplyr") ; library("caret")
library("purrr") ; library("XLConnect") ; library("sqldf") ; library("doParallel")


#Choose option
# 2 3 4 5
date <- c(ymd("2017-03-01") , ymd("2017-04-01")  )
n <- 61

#ACT / CONSTANT
DL_DIS_OPT <- "CONST"
DAY_WEEK <- "WEEK"

################################################################## LOOP #################################################################################################
##########################################################################################################################################
##########################################################################################################################################
for(i in seq_along(date)){
  
  
  print(date[i]);print(n);
  date_temp <-  date[i]
  horizon <- n
  print(i)
  
  #------------------------------------- TRAINING SET-------------------------------------
  perf <- readRDS(file = "D://BackTesting//ACTUALs.rds")
  
  if(DAY_WEEK == "WEEK"){
    
    
    ifelse(DL_DIS_OPT == "ACT" , source("D:\\BackTesting\\DL_Discount_BT_FEN.R" , echo = TRUE) ,
           source("D:\\BackTesting\\DL_Discount_BT_FEN_CONST.R" , echo = TRUE) )
    
  } else{
    #RUN DAY VERSION
    #CANNOT MIX 
    source("D:\\BackTesting\\DL_Discount_BT_FEN_CONST_Daily.R" , echo = TRUE) 
    
  }  
  #------------------------------------- GET DL-------------------------------------
  setwd("D://Model_Input//")
  
  if(DAY_WEEK == "WEEK"){
    
    if(DL_DIS_OPT == "ACT"){ 
      MBL <- readRDS("TEST_DL_FEN_ACT.rds")
      perf$weekno <- week(perf$booking_date)
      perf$yearno <- year(perf$booking_date)
      perf <- left_join(perf , MBL , by = c("weekno" = "week" , "yearno" = "year" , "cname" = "cname" , "final_tg" = "final_tg"))
      
    } else { #CONSTANT
      MBL <- readRDS("TEST_DL_FEN_ACT.rds")
      perf$weekno <- week(perf$booking_date)
      perf$yearno <- year(perf$booking_date)
      perf <- left_join(perf , MBL , by = c("weekno" = "week" , "yearno" = "year" , "cname" = "cname" , "final_tg" = "final_tg"))
      
    }
  } else{ #DAILY
    MBL <- readRDS("TEST_DL_FEN_DAILY.rds")
    perf <- left_join(perf , MBL , by = c("booking_date" = "booking_date" , "cname" = "cname" , "final_tg" = "final_tg"))
  }
  
  
  #------------------------------------- GET Discount-------------------------------------
  
  if(DAY_WEEK == "WEEK"){
    
    if(DL_DIS_OPT == "ACT"){ 
      Discount <-  readRDS("TEST_DISCOUNT_FEN_ACT.rds")
      perf$weekno <- week(perf$booking_date)
      perf$yearno <- year(perf$booking_date)
      perf <- left_join(perf , Discount , by = c("weekno" = "week" , "yearno" = "year" , "cname" = "cname" , "final_tg" = "final_tg"))
      
    } else { #CONSTANT
      Discount <- readRDS("TEST_DISCOUNT_FEN.rds")
      perf$weekno <- week(perf$booking_date)
      perf$yearno <- year(perf$booking_date)
      perf <- left_join(perf , Discount , by = c("weekno" = "week" , "yearno" = "year" , "cname" = "cname" , "final_tg" = "final_tg"))
      
    }
  } else{ #DAILY
    Discount <- readRDS("TEST_DISCOUNT_FEN_DAILY.rds")
    perf <- left_join(perf , Discount , by = c("booking_date" = "booking_date" , "cname" = "cname" , "final_tg" = "final_tg"))
  }
  
  
  
  
  
  #------------------------------------- REPLACING NA WITH  0 -------------------------------------
  na_names <- names(perf)
  perf[c(na_names)][is.na(perf[c(na_names)])] <- 0
  
  
  
  perf <- mutate(perf, flag = ifelse( booking_date >= date_temp, "Forecast" , "Actual"))
  SetZero <- c("gross_bkgs" , "gross_margin" , "gross_rn" , "gross_rnpb" , "gross_mpb" , "gross_abv"
               ,"gross_bkgs_ACT" , "gross_margin_ACT" , "gross_rn_ACT" , "gross_rnpb_ACT" 
               , "gross_mpb_ACT" , "gross_abv_ACT")
  perf[c(SetZero)][perf$booking_date >= date_temp ,] <- 0 
  
  max(perf$booking_date)
  source("D://BackTesting//DEV_ROIL14D.r" , echo = TRUE)
  
  ROI_L14D <- readRDS("D://Model_Result//ROI_L14D.rds") 
  perf <- left_join(perf ,  ROI_L14D , by = c("booking_date" = "Date" , "final_tg" = "final_tg"))
  perf$ROI_L14D <- ifelse(is.na(perf$ROI_L14D) | !is.finite(perf$ROI_L14D) , 0 , perf$ROI_L14D)
  
  
  train <- filter(perf , booking_date <= date_temp -1 )
  train <- filter(train , year(ymd(booking_date)) > 2013)
  
  
  #------------------------------------- KEEP ACTUAL SINCE ANOMALY IS REMOVED------------------------------------- 
  train$ACTUAL_BKGS <- train$gross_bkgs
  train$ACTUAL_rnpb <- train$gross_rnpb
  train$ACTUAL_mpb <- train$gross_mpb
  train$ACTUAL_abv <- train$gross_abv
  
  #------------------------------------- SPLIT BY FEN------------------------------------- 
  
  list_country_channel <- split(train  , list(train$cname , train$final_tg))
  list_country_channel <- list_country_channel[lapply(list_country_channel,nrow)>30]
  
  
  
  #------------------------------------- REPLACING NA WITH  0 ------------------------------------- 
  
  list_country_channel <- lapply(list_country_channel , function(x) {
    x$ROIs <- ifelse(is.na(x$ROIs) , 0 ,  x$ROIs)
    x$percent_downlift <- ifelse(is.na(x$percent_downlift) , 0 ,  x$percent_downlift)
    x$percent_discount <- ifelse(is.na(x$percent_discount) , 0 ,  x$percent_discount)
    return(x)
  }
  )
  
  #train all to date -1
  print("TRAIN")
  #========================================= GB ================================================================================================
  target <- paste0("gross_bkgs ~ ")
  
  trend <- paste0("avg_l4w_gbkgs + ly_spr +")
  
  sesonality_event <- paste0(" weekday + NY_Post + NY_Pre + CH_Post + CH_Pre + SK_Post + SK_Pre + 
                             BRM_Pre + BRM_Post + CNY_Post + CNY_Pre + ES_Pre + ES_Post + MID_POST + 
                             MID_PRE + MAYDAY_POST + MAYDAY_PRE + MERs + THBOMB + THKING + THcoup + DRM + ")
  
  mkt_act <- paste0("percent_downlift + percent_discount + ROIs + ROI_L14D")
  
  #CREATE formula for glm
  formula_gbkgs <- as.formula(gsub("[\r\n]" , "" , paste0(target , trend , sesonality_event , mkt_act)))
  
  #USE MULTICORE
  c1 <- makePSOCKcluster(20)
  setDefaultCluster(c1)
  
  #NEED TO EXPORT PACKAGE TO EACH CORE
  clusterEvalQ(NULL , library("caret"))
  clusterExport(cl= c1 , varlist=c("formula_gbkgs"))
  
  system.time(model2 <- parLapply(NULL , list_country_channel ,
                                  function(x)
                                    train(formula_gbkgs , data = x  , method = "glm")))
  
  #coef_gb <- data.frame(map(map(model2 , "finalModel"),"coefficients"))
  # list_country_channel <- map2(list_country_channel , model2 , function(x , y) 
  #   cbind(x , 
  #         data.frame(
  #           PRED_bkgs = 
  #             y$finalModel$fitted.values
  #         )
  #   )
  # )
  
  #========================================= RNPB ================================================================================================#
  
  #GET RMSE
  RMSE <- do.call("rbind",map((map(model2 , "results")) , "RMSE"))
  
  target <- paste0("gross_rnpb ~ ")
  
  trend <- paste0("avg_l4w_rnpb + ly_spr_rnpb + grnpb_ly +")
  
  
  sesonality_event <- paste0(" weekday + NY_Post + NY_Pre + CH_Post + CH_Pre + SK_Post + SK_Pre + 
                             BRM_Pre + BRM_Post + CNY_Post + CNY_Pre + ES_Pre + ES_Post + MID_POST + 
                             MID_PRE + MAYDAY_POST + MAYDAY_PRE + MERs + THBOMB + THKING + THcoup + DRM ")
  
  
  formula_rnpb <- as.formula(gsub("[\r\n]" , "" , paste0(target , trend , sesonality_event )))
  clusterExport(cl= c1 , varlist=c("formula_rnpb"))
  
  
  system.time(
    model2_rnpb <- parLapply(NULL , list_country_channel , 
                             function(x) train(formula_rnpb  , data = x  ,method = "glm"
                             ) )
  )
  
  # list_country_channel <- map2(list_country_channel , model2_rnpb , function(x , y) 
  #   cbind(x , 
  #         data.frame(
  #           PRED_rnpb = 
  #             y$finalModel$fitted.values
  #         )
  #   )
  # )
  
  #========================================= mpb ================================================================================================#
  target <- paste0("gross_mpb ~ ")
  
  trend <- paste0("avg_l4w_mpb + ly_spr_mpb + gmpb_ly +")
  
  
  sesonality_event <- paste0(" weekday + NY_Post + NY_Pre + CH_Post + CH_Pre + SK_Post + SK_Pre + 
                             BRM_Pre + BRM_Post + CNY_Post + CNY_Pre + ES_Pre + ES_Post + MID_POST + 
                             MID_PRE + MAYDAY_POST + MAYDAY_PRE + MERs + THBOMB + THKING + THcoup + DRM  + ")
  
  
  mkt_act <- paste0("percent_downlift + percent_discount")
  
  formula_mpb <- as.formula(gsub("[\r\n]" , "" , paste0(target , trend , sesonality_event , mkt_act)))
  clusterExport(cl= c1 , varlist=c("formula_mpb"))
  
  
  system.time(
    model2_mpb <- parLapply(NULL , list_country_channel , 
                            function(x) train( formula_mpb , data = x  ,method = "glm") 
    )
  )
  
  
  # list_country_channel <- map2(list_country_channel , model2_mpb , function(x , y) 
  #   cbind(x , 
  #         data.frame(
  #           PRED_mpb = 
  #             y$finalModel$fitted.values
  #         )
  #   )
  # )
  
  #========================================= abv ================================================================================================#
  target <- paste0("gross_abv ~ ")
  
  trend <- paste0("avg_l4w_abv + ly_spr_abv + gabv_ly +")
  
  sesonality_event <- paste0(" weekday + NY_Post + NY_Pre + CH_Post + CH_Pre + SK_Post + SK_Pre + 
                             BRM_Pre + BRM_Post + CNY_Post + CNY_Pre + ES_Pre + ES_Post + MID_POST + 
                             MID_PRE + MAYDAY_POST + MAYDAY_PRE + MERs + THBOMB + THKING + THcoup + DRM ")
  
  formula_abv <- as.formula(gsub("[\r\n]" , "" , paste0(target , trend , sesonality_event )))
  clusterExport(cl= c1 , varlist=c("formula_abv"))
  
  
  system.time(
    model2_abv <- parLapply(NULL , list_country_channel , 
                            function(x) train( formula_abv , data = x  ,method = "glm") )
  )
  
  # 
  # list_country_channel <- map2(list_country_channel , model2_abv , function(x , y) 
  #   cbind(x , 
  #         data.frame(
  #           PRED_abv = 
  #             y$finalModel$fitted.values
  #         )
  #   )
  # )
  
  #GET RMSE
  #map((map(model2 , "results")) , "RMSE")
  #========================================= save coef ================================================================================================#
  
  # coef_gb <- data.frame(t(data.frame(map(map(model2 , "finalModel"),"coefficients"))))
  # coef_gb$FEN <- paste0("GB_",row.names(coef_gb))
  # 
  # coef_rnpb <- data.frame(t(data.frame(map(map(model2_rnpb , "finalModel"),"coefficients"))))
  # coef_rnpb$FEN <- paste0("RNPB_",row.names(coef_rnpb))
  # 
  # coef_mpb <- data.frame(t(data.frame(map(map(model2_mpb , "finalModel"),"coefficients"))))
  # coef_mpb$FEN <- paste0("MPB_",row.names(coef_mpb))
  # 
  # coef_abv <- data.frame(t(data.frame(map(map(model2_abv , "finalModel"),"coefficients"))))
  # coef_abv$FEN <- paste0("ABV_",row.names(coef_abv))
  # 
  # coef_final <- bind_rows(coef_gb , coef_rnpb , coef_mpb , coef_abv)
  # saveRDS(coef_final , "Direct_coef.rds")
  
  ##=========================================  prediction ================================================================================================#
  
  to_predict <- perf
  
  to_predict <- arrange(to_predict , booking_date)
  to_predict <- filter(to_predict , booking_date <= date_temp + n)
  
  predict_list_country_channel <- split(to_predict  , list(to_predict$cname , to_predict$final_tg))
  predict_list_country_channel <- predict_list_country_channel[lapply(predict_list_country_channel,nrow) > 30]
  
  
  predict_list_country_channel <-  predict_list_country_channel[names(predict_list_country_channel) %in% names(model2)]
  
  
  
  predict_list_country_channel <- pmap(list(predict_list_country_channel , model2 , model2_abv , model2_mpb , model2_rnpb) ,
                                       function(a , b , c , d , e) list(a , b , c, d, e ))
  # 
  # #GB MODEL OF 1st FEN
  # predict_list_country_channel[[1]][[2]]
  # #ABV MODEL OF 1st FEN
  # predict_list_country_channel[[1]][[3]]
  # #MPB MODEL OF 1st FEN
  # predict_list_country_channel[[1]][[4]]
  # #RNPB MODEL OF 1st FEN
  # predict_list_country_channel[[1]][[5]]
  #Make a rolling prediction i.e. t+1 average is a function of actual and predicted numbers.
  
  
  ##=========================================  PARALLEL LOOP ================================================================================================
  print("##################################### PREDICTION ###########################################################")
  system.time(
    predict_list_country_channel <-  parLapply(NULL , predict_list_country_channel , function(x){
      #FOR EACH LEVEL
      for (i in 1:nrow(x[[1]])){
        #FOR EACH Booking DATE
        
        if(x[[1]]$flag[i] == "Actual"){
          next
        }
        #x[[1]]$gross_bkgs[i] <- 1
        
        x[[1]]$gross_bkgs[i] <- predict(x[[2]] , x[[1]][i, ])
        x[[1]]$gross_abv[i] <- predict(x[[3]] , x[[1]][i, ])
        x[[1]]$gross_mpb[i] <- predict(x[[4]] , x[[1]][i, ])
        x[[1]]$gross_rnpb[i] <- predict(x[[5]] , x[[1]][i, ])
        
        
        # predict_list_country_channel[[1]][[1]]$gross_bkgs[10]
        # predict(predict_list_country_channel[[1]][[2]] , predict_list_country_channel[[1]][[1]][10,])
        # UPDATE Last 7 days 
        
        if (i < nrow(x[[1]]) & i-7 > 0 ){
          
          x[[1]]$avg_l4w_gbkgs[i+1] <- mean(c(x[[1]]$gross_bkgs[i-6] , x[[1]]$gross_bkgs[i-13] , x[[1]]$gross_bkgs[i-20] , x[[1]]$gross_bkgs[i-27]) , na.rm=TRUE )
          x[[1]]$avg_l4w_abv[i+1] <- mean(c(x[[1]]$gross_abv[i-6] , x[[1]]$gross_abv[i-13] , x[[1]]$gross_abv[i-20] , x[[1]]$gross_abv[i-27]) , na.rm=TRUE )
          x[[1]]$avg_l4w_mpb[i+1] <- mean(c(x[[1]]$gross_mpb[i-6] , x[[1]]$gross_mpb[i-13] , x[[1]]$gross_mpb[i-20] , x[[1]]$gross_mpb[i-27]) , na.rm=TRUE )
          x[[1]]$avg_l4w_rnpb[i+1] <- mean(c(x[[1]]$gross_rnpb[i-6] , x[[1]]$gross_rnpb[i-13] , x[[1]]$gross_rnpb[i-20] , x[[1]]$gross_rnpb[i-27]) , na.rm=TRUE )
          
        }
        
        
      }
      #IMPORTANT!! IF DONE SEQUENCIALLY THE NEXT LINE IS NOT NEEDED, BUT SINCE WE DO IT IN PARALLEL NEED TO SPECIFY WHAT IS RETURN
      
      x[[1]]
    }
    )
  ) 
  
  #=========================================  Summary and clean up ================================================================================================#=========================================
  #Summarize result
  predict_list_country_channel <- lapply(predict_list_country_channel ,
                                         function(x){
                                           x$gross_margin <- x$gross_mpb*x$gross_bkgs
                                           x$gross_rn <- x$gross_rnpb * x$gross_bkgs
                                           x$gross_amount <- x$gross_abv * x$gross_bkgs
                                           return(x) })
  
  predict_list_country_channel <- lapply(predict_list_country_channel , 
                                         function(x) mutate(x , lag_bkg_1_to_7 = x$gross_bkgs   +  lag(x$gross_bkgs , 1 ) 
                                                            +  lag(x$gross_bkgs , 2 ) 
                                                            +  lag(x$gross_bkgs , 3 ) 
                                                            +  lag(x$gross_bkgs , 4 ) 
                                                            +  lag(x$gross_bkgs , 5 ) 
                                                            +  lag(x$gross_bkgs , 6 ) 
                                                            ,  lag_date_6 = lag(x$booking_date , 6 )
                                                            , lag_margin_1_to_7 = x$gross_margin + 
                                                              lag(x$gross_margin , 1 ) + 
                                                              lag(x$gross_margin , 2 ) + 
                                                              lag(x$gross_margin , 3 ) + 
                                                              lag(x$gross_margin , 4 ) +
                                                              lag(x$gross_margin , 5 ) + 
                                                              lag(x$gross_margin , 6 )
                                                            , lag_date_6_margin = lag(x$gross_margin , 6 )
                                                            , lag_rn_1_to_7 = x$gross_rn + 
                                                              lag(x$gross_rn , 1 ) + 
                                                              lag(x$gross_rn , 2 ) + 
                                                              lag(x$gross_rn , 3 ) + 
                                                              lag(x$gross_rn , 4 ) + 
                                                              lag(x$gross_rn , 5 ) + 
                                                              lag(x$gross_rn , 6 )
                                                            , lag_date_6_rns = lag(x$gross_rn , 6 )
                                                            , lag_amount_1_to_7 = x$gross_amount + 
                                                              lag(x$gross_amount , 1 ) + 
                                                              lag(x$gross_amount , 2 ) + 
                                                              lag(x$gross_amount , 3 ) + 
                                                              lag(x$gross_amount , 4 ) +
                                                              lag(x$gross_amount , 5 ) + 
                                                              lag(x$gross_amount , 6 )
                                                            , lag_date_6_amount = lag(x$gross_amount , 6 )
                                                            
                                         )
  )
  
  final_gross <- bind_rows(predict_list_country_channel)
  row.names(final_gross) <- c()
  
  # getwd()
  # write.csv(final_gross , "prediction.csv" , row.names = FALSE)
  ############################### TEMP CHECK ####################################################################
  # tempp <- group_by(final_gross , m = month(booking_date) , y = year(booking_date) , final_tg , cname)
  # tempp <- summarise(tempp , sum(gross_bkgs) )
  # data.frame(arrange(tempp, final_tg)) -> test
  
  final_gross$gross_bkgs <- ifelse(final_gross$gross_bkgs_ACT != 0 , final_gross$gross_bkgs_ACT ,  final_gross$gross_bkgs)
  final_gross$gross_margin <- ifelse(final_gross$gross_margin_ACT != 0 , final_gross$gross_margin_ACT ,  final_gross$gross_margin)
  final_gross$gross_rn <- ifelse(final_gross$gross_rn_ACT != 0 , final_gross$gross_rn_ACT ,  final_gross$gross_rn)
  final_gross$gross_rnpb <- ifelse(final_gross$gross_rnpb_ACT != 0 , final_gross$gross_rnpb_ACT ,  final_gross$gross_rnpb)
  final_gross$gross_mpb <- ifelse(final_gross$gross_mpb_ACT != 0 , final_gross$gross_mpb_ACT ,  final_gross$gross_mpb)
  final_gross$gross_abv <- ifelse(final_gross$gross_abv_ACT != 0 , final_gross$gross_abv_ACT ,  final_gross$gross_abv)
  final_gross$gross_amount <- final_gross$gross_bkgs * final_gross$gross_abv 
  final_gross <- select(final_gross , - gross_bkgs_ACT , - gross_margin_ACT , -gross_rn_ACT
                        , -gross_rnpb_ACT , -gross_mpb_ACT , -gross_abv_ACT , -final_tg_ACT  , -cname_ACT , -booking_date_ACT )
  
  
  
  #TO BE USED IN CXL AND DEPARTED
  setwd("D://BackTesting")
  saveRDS(final_gross , file = paste0(date_temp ,"-", "RAW_Gross_model"))
  
  
  #write.csv(final_gross , paste0("Gross_model.csv" ), row.names =  FALSE )
  # write.csv(test , paste0(date_temp ,"-" , "Monthly_result.csv" ), row.names =  FALSE )
  temp <- filter(final_gross , booking_date >= date_temp , booking_date <= date_temp + n)
  temp <- group_by(temp , final_tg, cname, booking_date ) %>% summarise("GB" = sum(gross_bkgs)) %>% data.frame()
  
  #save(final_gross, file = paste0("Gross_model.RData"))
  ################################## RESULT BY DAY #####################################################################
  #GET ACTUAL
  ACT_DATA <- readRDS("ACTUALs.rds")
  ACT_DATA <- filter(ACT_DATA , booking_date >= date_temp , booking_date <= date_temp + n)
  ACT_DATA <- group_by(ACT_DATA , final_tg, cname, booking_date ) %>% summarise("GB" = sum(gross_bkgs)) %>% data.frame()
  
  #GET FINAL
  result <- left_join(temp , ACT_DATA , by = c("final_tg" = "final_tg", "cname" = "cname", "booking_date"= "booking_date" ))
  names(result) <- c("final_tg" , "cname", "booking_date", "FCST" , "ACT")
  write.csv(result , paste0(date_temp, "_" , n ,"RESULT.csv"))
  
  ################################## RESULT BY PERIOD #####################################################################
  temp2 <- filter(final_gross , booking_date >= date_temp , booking_date <= date_temp + n)
  temp2 <- group_by(temp2 , final_tg  ) %>% summarise("GB" = sum(gross_bkgs)) %>% data.frame()
  
  ACT_DATA2 <- readRDS("ACTUALs.rds")
  ACT_DATA2 <- filter(ACT_DATA2 , booking_date >= date_temp , booking_date <= date_temp + n)
  ACT_DATA2 <- group_by(ACT_DATA2 , final_tg ) %>% summarise("GB" = sum(gross_bkgs)) %>% data.frame()
  
  #GET FINAL
  result2 <- left_join(temp2 , ACT_DATA2 , by = c("final_tg" = "final_tg" ))
  names(result2) <- c("final_tg" ,  "FCST" , "ACT")
  write.csv(result2 , paste0(date_temp, "_" , n ,"_AGG_RESULT.csv"))
  
  stopCluster(c1)
}

#ANALYSIS
# result <- mutate(result, "diff" = (FCST/ACT-1)*100)
# result_temp <- filter(result, final_tg=="Direct App")
# result_temp <- filter(result_temp, complete.cases(result_temp))
# ggplot(result_temp, aes(x=booking_date, y=diff))+geom_line() + facet_wrap(~cname)
# ggplot(result_temp, aes(x=cname, y=diff))  + geom_violin() + geom_boxplot(width = 0.3)


