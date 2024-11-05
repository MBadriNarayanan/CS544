FROM p5-base
CMD ["bash", "-c", "/spark-3.5.3-bin-hadoop3/sbin/start-worker.sh spark://boss:7077 -c 1 -m 512M && tail -f /dev/null"]