#!/bin/bash
# need HBase admin access to create htable.

echo "truncate 'test.vertex'" | hbase shell
echo "truncate 'test.edge'" | hbase shell
