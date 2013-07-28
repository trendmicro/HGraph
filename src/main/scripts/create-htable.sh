#!/bin/bash
# need HBase admin access to create htable.

echo "create 'test.vertex', {NAME => 'property', BLOOMFILTER => 'ROW', COMPRESSION => 'GZ', TTL => '7776000'}" | hbase shell
echo "describe 'test.vertex'" | hbase shell

echo "create 'test.edge', {NAME => 'property', BLOOMFILTER => 'ROW', COMPRESSION => 'GZ', TTL => '7776000'}" | hbase shell
echo "describe 'test.edge'" | hbase shell
