mkdir -p ~/.ssh
mv -f ~/labsuser.pem ~/.ssh/vockey.pem
chmod 400 ~/.ssh/vockey.pem
ssh -i ~/.ssh/vockey.pem hadoop@<primary-node-public-dns>

pip3 install --user kaggle
export PATH=$PATH:~/.local/bin
mkdir -p ~/.kaggle
echo '{"username":"<username>","key":"<api-key>"}' > ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
 
mkdir -p /mnt1/steam && cd /mnt1/steam
kaggle datasets download -d najzeko/steam-reviews-2021
unzip steam-reviews-2021.zip && rm -f steam-reviews-2021.zip

cd /mnt1/steam
head -n 1000001  steam_reviews.csv > s1m.csv
head -n 5000001  steam_reviews.csv > s5m.csv
head -n 10000001 steam_reviews.csv > s10m.csv

for n in 1000001 5000001 10000001; do
  case $n in
    1000001)  out=s1m_fix.csv ;;
    5000001)  out=s5m_fix.csv ;;
    10000001) out=s10m_fix.csv ;;
  esac
  head -n $n steam_reviews.csv | head -n -50 > $out
done

hdfs dfs -mkdir -p /user/hadoop/steam
hdfs dfs -put steam_reviews.csv s1m.csv s5m.csv s10m.csv /user/hadoop/steam/
hdfs dfs -ls -h /user/hadoop/steam/


