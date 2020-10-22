## How to use meta-data-upgrade tool to upgrade from the 1.0 meta data to 2.0 meta data

- Step 1: Get the 1.0 meta data path from your nebula-metad.conf.
- Step 2: Run the tool
```
./meta_data_update -meta_data_path=<your meta data path> -null_type=<true: the schema support null type; false: the schema should not be null type>
```
- Step 3: The output log will tell you the 2.0 meta data path,
          and you need modify the 2.0 metad's conf file, set `--data_path` with the 2.0 meta data path.
