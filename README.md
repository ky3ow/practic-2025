# Intro

This project is responsible for deploying AWS in Databricks using Terraform, and organizes the daily processing (ETL pipeline) and visualization of weather data using DevOps and IAC approaches.

After processing, for demonstration, the data in Databricks was queried from PowerBI.

# File structure

```
.
├── README.md
├── .github/workflows/
│   └── deploy.yml
├── databricks
│   ├── databricks.yml
│   └── process_weather.py
└── terraform
    ├── databricks.tf
    ├── iam.tf
    ├── outputs.tf
    ├── providers.tf
    ├── root_storage.tf
    ├── variables.tf
    ├── vpc.tf
    └── workspaces.tf
```

# Terraform

The environment itself is deployed using Terraform.

To do this, you need to set up authentication via the `aws cli` and activate a Databricks subscription on the AWS Marketplace.

Next, you need to run the following commands

```bash
cd terraform
terraform init
terraform apply
```

After that, the Databricks environment will be ready, and a link to it will be displayed.

# Databricks

Databricks is configured with Unity Catalog, the sandbox catalog data is stored in an S3 bucket using External Location and Credentials; data processing and table creation (if tables do not exist) are performed using the `databricks/process_weather.py` script on a laptop and run on a daily schedule, organized via Jobs deployed using `databricks bundle deploy` (`databricks/databricks.yml`)

# Architecture

![architecture](./architecture.png)

# Images

As this was shut down to not accumulate costs, here are screenshots of what was achieved:


<img width="2035" height="531" alt="image" src="https://github.com/user-attachments/assets/876878f3-a682-4703-a20b-4bb351558e61" />


Deployed databricks via IAC

<img width="2067" height="935" alt="image" src="https://github.com/user-attachments/assets/29246b6c-f28e-4e12-9da5-00f898fffb2f" />


Job running which was deployed from github actions by databricks asset bundle



<img width="2008" height="419" alt="image" src="https://github.com/user-attachments/assets/a9d1847d-f680-4e55-9998-42dd32d33d48" />


Data residing in S3 bucket (and not in databricks-managed location, which is important)



<img width="1258" height="997" alt="image" src="https://github.com/user-attachments/assets/0fce8b2c-f39a-44ff-9eca-6ca5d8dab6d3" />


Connecting powerbi to data in databricks



<img width="1302" height="592" alt="image" src="https://github.com/user-attachments/assets/f53e1316-fce8-4b57-9a4e-a10a7898c10c" />


Sample report on the gathered data
