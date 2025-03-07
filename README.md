# dbt-ibm-netezza

The `dbt-ibm-netezza` package contains all of the code required to make `dbt` operate on a Netezza database. For more information on using dbt, consult [their docs](https://docs.getdbt.com/docs).


### Performance Optimizations

Tables in Netezza have an optimization to improve query performance called distribution keys. Supplying these values as model-level configurations apply the corresponding settings in the generated `CREATE TABLE` DDL. Note that these settings will have no effect for models set to view or ephemeral models.

- `dist` can take a setting of `random`, a single column as a string (e.g. `visit_key`), or a list of columns (e.g. `['visit_key','visit_event_key']`)

Dist keys can be added to the `{{ config(...) }}` block for a specific model `.sql` file, e.g.:

```sql
-- Example with one sort key
{{ config(materialized='table', dist='visit_key') }}

select ...


-- Example with multiple sort keys
{{ config(materialized='table', dist=['visit_key', 'visit_event_key']) }}

select ...
```

Dist keys can also be added to the `dbt_project.yml` file config to set a default, e.g. 

```yaml
# dbt_project.yml
name: "my_project"
version: "0.0.1"
config-version: 2

...

models:
  my_project:
    +materialized: table
    +dist: random
```

# Testing Sample dbt project

## Installation Guide

To install all the dependencies for the tool, follow these steps:

1. Navigate to the `nz-dbt` directory:

    ```bash
    cd nz-dbt
    ```

2. Install `dbt-ibm-netezza` using the command `pip install .`

Initialize a new dbt project using command `dbt init` and provide all the informantion prompted like project_name, hostname, database, etc. The details you put are case-sensitive.

This will create the configuration of your project inside the file with path

```
$HOME/.dbt/profiles.yml
```

The configuration should look like:

```yaml
dbtnzsampleproject:
  outputs:
    dev:
      database: '"sampledb"'
      host: my_host
      password: 
      port: 5480
      schema: sampleschema
      threads: 1
      type: netezza
      user: '"ADMIN"'
  target: dev
```

> **Note:** 
> 
> We provide the database name and the user name inside double quotes in order to make it case sensitive. Other objects like schema is also case sensitive.

Check your dbt connection with netezza using the command :
```bash
dbt debug
```

Create the tables into your db using the info in the `datainsertion.sql` file.

> **Note:** 
> 
> Take note that we would be using the names of the tables created into our database as it is ,i.e., the tables created would be created as CUSTOMERS, ORDERS and PAYMENTS so we would use names of these objects in dbt as it is.

We can load the data into our tables using the `dbt seed command` , it would insert the data from all the seed files into tables created with the name of the seed files.

Before using the `dbt seed` command, ensure that you have provided an `et_options.yml` file in your dbt project folder. This file is crucial for configuring the parameters for inserting data from an external file into your table.

> **Note:** 
> 
> The `et_options.yml` file allows you to specify the parameters for inserting data from an external source according to your needs. For detailed information on how to configure the `et_options.yml` file and the available options, refer to the Netezza documentation here: [Netezza Option Details](https://www.ibm.com/docs/en/netezza?topic=options-option-details).

Make sure your `et_options.yml` file is correctly set up in your dbt project folder before running the `dbt seed` command. This ensures that data is inserted into your tables accurately as specified in the external file.

The file should look like:

```yaml
- !ETOptions
    SkipRows: "1"
    Delimiter: "','"
    DateDelim: "'-'"
    MaxErrors: " 0 "
```

## Working with dbt Models

### Creating Models

You can create models as specified in our sample project. Models define the transformations and logic for your data.

### Running Models

To execute your dbt models, use the `dbt run` command. This command will run all the models defined in your dbt project.

```bash
dbt run
```

#### Running Specific Models

If you want to run a specific model instead of all models, you can specify it using the `--select` option. For example, to run the `stg_customers model`, use:
```bash
dbt run --select stg_customers
```

### Testing Models

After running your models, it is important to test the outputs to ensure they meet your expectations. Use the dbt test command to run all the tests defined in your dbt project.

After running the models we can run the `dbt test` command to test the output of the models.
```bash
dbt test
```

#### Testing Specific Models
To test a specific model, you can use the `--select` option with the dbt test command. For example, to test the `stg_payments` model, use:

```bash
dbt test --select stg_payments
```

We can generate docs using command:
```
dbt docs generate
```

We can view the documentation for the project using the command:
```
dbt docs serve
```

## Using dbt Snapshot with Netezza

We can utilize the snapshot functionality provided by dbt to track historical changes to our data. However, to use this feature, you first need to install the SQL Extension Toolkit for Netezza on your database.

> **Note:** 
> 
> The SQL Extension Toolkit must be installed on the default `ADMIN` schema of your database. In this case, the database is `sampledb`. For detailed instructions on how to install the SQL Extension Toolkit, refer to the Netezza documentation here: [SQL Extension Toolkit Installation](https://www.ibm.com/docs/en/netezza?topic=analytics-sql-extensions-toolkit).

Once the toolkit is installed, you can use the `dbt snapshot` command to capture historical data changes.

### Steps to Use `dbt snapshot`

1. **Install SQL Extension Toolkit**:
   Ensure the toolkit is installed on the `ADMIN` schema of `sampledb` as outlined in the provided documentation.

2. **Run the `dbt snapshot` Command**:
   After installation, you can proceed with running the `dbt snapshot` command to create snapshots of your data.

   ```bash
   dbt snapshot
