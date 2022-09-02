# Install Java
sudo apt update
sudo apt install -y openjdk-16-jre openjdk-16-jdk

# Setup for maven install
mvn_version=${mvn_version:-3.8.6}
url="http://www.mirrorservice.org/sites/ftp.apache.org/maven/maven-3/${mvn_version}/binaries/apache-maven-${mvn_version}-bin.tar.gz"
install_dir="/opt/maven"

# Install maven
sudo mkdir ${install_dir}
sudo curl -fsSL ${url} | sudo tar zx --strip-components=1 -C ${install_dir}
sudo tee /etc/profile.d/maven.sh > /dev/null << EOF
#!/bin/sh
export MAVEN_HOME=${install_dir}
export M2_HOME=${install_dir}
export M2=${install_dir}/bin
export PATH=${install_dir}/bin:$PATH
EOF
source /etc/profile.d/maven.sh
echo maven installed to ${install_dir}
mvn --version
echo "source /etc/profile.d/maven.sh" >>  ~/.bashrc

# Install minizinc
sudo apt install -y minizinc
