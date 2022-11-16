# GBFS Watcher

A daemon which logs Velib' stations statuses and provides a REST endpoint to
access data.

The goal of the project is to be as lightweight and portable as possible to
allow running it for years with decent latency on a Raspberry Pi, possibly
using a bad to average USB hard drive.

This requirement comes from a previous attempt for a similar project backed on
SQLite which became much too slow on such hardware after a few month of data
was ingested (SQLite despite its bad reputation should be quite competitive for
this task).
