# dies-destribution
>This is a program to solve the destribution of the material of the dies. Because during destribution, there are always some batch left a little. When the next device choose which device should be used, the left batched will be picked up because of some rule. Therefore, the schedule of the device will become fragmental. The effect will reflected on the production line. In order to solve this problem, the dynamic program is applied to this program. After using DP to choose the batch appropriately, the final inventory will have the least left batches and the schedule will become most appropriate.
>In this program, there are several files. Each has it own function to deal with different part.

* DeviceSetting.py
> This is used to read the material of device corresponded to the batch.

* DieRelease.py
> This is used to read the rules about the number of limit that when the usage of the batch or wafer is over the limit, the batch or wafer should be destributed all.
