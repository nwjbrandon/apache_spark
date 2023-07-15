brew install openjdk@11 
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
brew install scala     
brew install apache-spark  
conda env create -f environment.yml

wget http://media.sundog-soft.com/es/ml-100k.zip 