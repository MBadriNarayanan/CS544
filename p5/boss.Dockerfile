FROM p5-base
CMD ["bash", "-c", "/spark-3.5.3-bin-hadoop3/sbin/start-master.sh && tail -f /dev/null"]