# dies-destribution
>This is a program to solve the destribution of the material of the dies. The job is used to finish by employees. The employee need to trace a lot of different rule and choose appropriate batch in inventory. This is only regualr process for one device. If there is lot of device, it will be quite time-consuming. The goal is reduce the human resources and improve the effective on using the batch. Because during destribution, there are always some batch left a little. When the next device choose which device should be used, the left batched will be picked up because of some rule. Therefore, the schedule of the device will become fragmental. The effect will reflected on the production line. In order to solve this problem, the dynamic program is applied to this program. After using DP to choose the batch appropriately, the final inventory will have the least left batches and the schedule will become most appropriate.
>In this program, there are several files. Each has it own function to deal with different part.

* DeviceSetting.py
> This is used to read the material of device corresponded to the batch.

* DieRelease.py
> This is used to read the rules about the number of limit that when the usage of the batch or wafer is over the limit, the batch or wafer should be destributed all.

* DynamicProgram.py
> This is used to execute the whole distribution. Read rules, exclude unsuitable batch, destribution are all included.

* Knapsack.py
> This is dinamic program. Use Knapsack to choose suitable batch and make the total left die as much as close to the demand.

* Log.py
> This is used to transform the log from .json to readable dataframe.

* Message.py
> This is used to add the destribution and split schedule message to the result. The message is about the situlation during the whole destrubution.

* PID.py
> This it use to read the device corresponded to the material.

* RWcsv.py
> This is use to read data from .csv file

* SameGroup.py
> This is use to group the device that use the same material.

* SetEng.py
> This is use to exclude the batch from inventory which has the eng flag.

* SplitSchedule.py
> This is used to split the schedule after destribution. The factory will use the scheduel to produce the device. How much to produce and which batch should be taken will be recoded in the schedule. 

* SplitWaferID.py
> The wafer ID is usuallt a serial number. This is used to saperate the two-digits number from the serial number.

* demandBuildPlan.py
> This is used to formatted the demand build plan.

* main.py
> This is the entry of the program.

* orcl_connect.py
> This is uesd to build the connection to the oracle data base. There are several function to get the specific rule from the DB.

