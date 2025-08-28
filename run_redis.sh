
#!/usr/bin/bash 
. /software/scipion/conda/etc/profile.d/conda.sh
conda activate redis-server
cd /home/irene/.emhub/instances/test && redis-server redis.conf --daemonize yes
