# verylong-string-matching-in-hadoop
Extremely long string matching in Hadoop.

Given an extremely long string without '\n' or any delimiter containing only 4 characters, including a,b,c, and d in multiple 1 Gigabyte files.

The goal is to find the "Exact" match of the given pattern in give multiple 1 Gigabyte files using Hadoop.

Work are separated in 3 tasks.

  1. Brute-force using single computer.
  1. Brute-force using multiple computer using Hadoop.
  1. Make it better from the previous two task using Hadoop.

**Progress**

  1. DONE, use code from #2 but only use 1 node.
  1. Successfully implemented. Support 1GB file already. ( Extremely Slow )
  1. Successfully implemented. Support 1GB file already. ( A lot Faster than #2 )

`._.`
