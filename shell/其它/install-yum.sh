iptables -F

echo http_proxy=http://192.168.80.101:3128>>/etc/profile
echo export http_proxy>>/etc/profile

source /etc/profile

yum -y install lrzsz
yum -y install telnet
yum -y install openssl-devel
yum -y install pcre-devel
yum -y install libart_lgpl
yum -y install redhat-lsb
