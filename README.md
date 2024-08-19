# Data Engineering Project
Built and evaluated the processing time of an Azure Event Grid.
The data-generator and data-parser folders contain the files of two azure functions displayed in the architecture image below.

Project mainly consists of two module folders:
- data-generator, which is generating the user data that are being uploaded to the Azure blob.
- data-parser, which parses the user data and and appends the data into a Azure PostgreSQL database.
![Alt text](/architecture_image.png)
