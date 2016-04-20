
##  setwd("C:/.../GasData")
##  load("Gas.RData")
## install.packages("RMySQL")

library(RMySQL)

source("calcRMSE.r")

mydb = dbConnect(MySQL(), user='root', password='pswd1234',
                                       dbname='Gas', host='localhost')

# to check the tables in the database 'Gas': 
dbListTables(mydb)

# to check the fields in the table ethylene_CO_seconds_timejoin2:
dbListFields(mydb, 'ethylene_CO_seconds_timejoin2')

# get the data out of MySQL:
timejoinQ2 = dbSendQuery(mydb,
             "select * from ethylene_CO_seconds_timejoin2 order by Timesec")

COtimejoin2 = fetch(timejoinQ2, n=-1)

#  so the data are retrieved here

# prepare the formulae for linear regression:
srnames1 = names(COtimejoin2)[4:19]
srnames1 = paste(srnames1, collapse=" + ")

srnames2 = names(COtimejoin2)[4:35]
srnames2 = paste(srnames2, collapse=" + ")

srnames3 = names(COtimejoin2)[4:51]
srnames3 = paste(srnames3, collapse=" + ")

fmCO1 = paste("CO", "~", srnames1)
fmCO2 = paste("CO", "~", srnames2)
fmCO3 = paste("CO", "~", srnames3)

# See how simple linear regression performs
lm1 = lm(as.formula(fmCO1), data=COtimejoin2)
predCO = predict(lm1)
# force non-negative:
predCO[predCO < 0] = 0
calcRMSE(predCO,COtimejoin2$CO)    # 87.48057

## See how incorporating the past second readings may help:

lm2 = lm(as.formula(fmCO2), data=COtimejoin2)
predCO2 = predict(lm2)
# force non-negative:
predCO2[predCO2 < 0] = 0
calcRMSE(predCO2,COtimejoin2$CO)    #  81.27388, improvement !!

## See how incorporating the past two seconds' readings may help:

lm3 = lm(as.formula(fmCO3), data=COtimejoin2)
predCO3 = predict(lm3)
# force non-negative:
predCO3[predCO3 < 0] = 0
calcRMSE(predCO3,COtimejoin2$CO)    #  79.95664, less improvement


# Then, see how Poisson regression may help:

# first, still use only the 16 readings

glm1 = glm(as.formula(fmCO1), data=COtimejoin2, family = "poisson")
pred.glm1 = predict(glm1, type="response")
calcRMSE(pred.glm1,COtimejoin2$CO)    # 87.20532



# Second, use the 32 readings, including the ones in the previous second.

glm2 = glm(as.formula(fmCO2), data=COtimejoin2, family = "poisson")
pred.glm2 = predict(glm2, type="response")
calcRMSE(pred.glm2,COtimejoin2$CO)    # 85.08451

# Second, use the 48 readings
glm3 = glm(as.formula(fmCO3), data=COtimejoin2, family = "poisson")
pred.glm3 = predict(glm3, type="response")
calcRMSE(pred.glm3,COtimejoin2$CO)    # 85.14778







