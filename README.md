# Hazelcast Hadoop Data Loader

A tool to load data from HDFS to Hazelcast IMDG.

Currently, the project only supports Comma Separated Files(CSV). Additional file formats will be added later.

`HazelcastLoader` will initiate a Map Reduce job to read out the lines in the map phase and writes output to the Hazelcast Map via Hazelcast Client.

You've need to configure Hazelcast Client to be able to make `HazelcastLoader` talk to Hazelcast Cluster.


# Usage

`bin/hadoop jar hazelcast-hadoop--<version>.jar HazelcastLoader -files /path/to/hazelcast-client.xml <input> <file-type> <key-index>`


# Configuration

You should run the jar as a typical Map Reduce job with the parameters
- `<input>` - the file/path which will be imported to Hazelcast
- `<file-type>` - the type of the file, for now it will be `csv`
- `<key-index>` - the field index in the CSV file which will be used as a key in the Hazelcast Map. Starts from zero.

You need to provide `hazelcast-client.xml` which includes your cluster credentials and a property called `hazelcast.target.map`,
which is the target map name for the loading process. An example `hazelcast-client.xml` can be found below.

```
<?xml version="1.0" encoding="UTF-8"?>
<!--
  ~ Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<hazelcast-client xsi:schemaLocation="http://www.hazelcast.com/schema/client-config hazelcast-client-config-3.6.xsd"
                  xmlns="http://www.hazelcast.com/schema/client-config"
                  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <group>
        <name>dev</name>
        <password>dev-pass</password>
    </group>
    <properties>
        <property name="hazelcast.target.map">import-map</property>
    </properties>
    <network>
        <cluster-members>
            <address>172.17.0.17:5701</address>
            <address>172.17.0.18:5701</address>
        </cluster-members>
    </network>
</hazelcast-client>
```

# Building the Project

To build the project should need to issue the command below

`mvn clean package`

The you can find the jar file named `hazelcast-hadoop-<version>.jar` under the `target` folder.