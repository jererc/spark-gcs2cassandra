Create a local_settings.json file to override the default conf:


        {
            "cassandraHost" : "localhost",
            "cassandraRpcPort" : "9160",
            "cassandraKeyspace" : "test_keyspace",
            "cassandraTable" : "test_table_output",
            "sparkMasterHost" : "localhost",
            "sparkMasterPort" : "7077",
            "gcsProjectId" : "",
            "gcsAccountEmail" : "",
            "gcsAccountKeyfile" : "",
            "gcsFilePrefix" : "",
            "resetCassandraKeyspace" : false,
            "resetCassandraTable" : false
        }

Run:

        ./sbt assembly run
