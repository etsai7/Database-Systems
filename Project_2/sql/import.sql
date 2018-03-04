-- Populate each of the TPCH tables using the sqlite '.import' meta-command.
-- Students no not need to modify this file.

-- Notes:
--   1) The csv file for <table> is located at '~cs416/datasets/hw0/tpch-sf0.1/<table>.csv'.

.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/part.csv part
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/supplier.csv supplier
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/partsupp.csv partsupp
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/customer.csv customer
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/orders.csv orders
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/lineitem.csv lineitem
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/nation.csv nation
.import /home/cs22610/db/hw3/test/datasets/tpch-tiny/region.csv region
