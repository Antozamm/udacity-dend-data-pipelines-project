class CreateTables:

    copy_stage_table = ("""
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        COMPUPDATE OFF 
        REGION '{}'
        JSON '{}';
    """)
    