@echo off

start /B python dataNode.py
start /B python nameNode.py
start python client.py