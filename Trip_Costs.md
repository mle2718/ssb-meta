# Predicted Trip costs

Predicted Trip costs are stored on the network.

Here is a bit of R code that you can use to read in that data:
```
socialsci_dir<-full.path.to.social.sci.directory

cost_directory1<-file.path(socialsci_dir,"Trip_Costs","2000-2009")
cost_directory2<-file.path(socialsci_dir,"Trip_Costs","Archived","2010-2021")
cost_directory_new<-file.path(socialsci_dir,"Trip_Costs","2010-2022")


Cost_Part1 <- read_excel(file.path(cost_directory1,"2000_2009_Commercial_Fishing_Trip_Costs.xlsx"), sheet=1)
Cost_Part2 <- read_excel(file.path(cost_directory1,"2000_2009_Commercial_Fishing_Trip_Costs.xlsx"), sheet=2)
Cost_Part3 <- read_excel(file.path(cost_directory_new,"2010_2022_Commercial_Fishing_Trip_Costs.xlsx"))
```
