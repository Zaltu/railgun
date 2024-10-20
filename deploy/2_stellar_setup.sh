echo "Performing initial StellarDB setup"

# DEBCONF_NOWARNINGS=yes

# echo "Installing encyrption requirements"
# apt-get -qq update && apt-get -qq install -y apache2-utils


PASS=$(openssl rand -base64 12)
HASH=$(htpasswd -bnB "" $PASS | cut -d ":" -f 2)

echo "The default admin password is:" $PASS
echo "The hash is:" $HASH

# echo "Removing encryption requirements"
# apt-get -qq remove -y apache2-utils
# apt-get -qq autoremove -y


echo "Adding admin user to StellarDB"

psql --username $POSTGRES_USER --dbname $POSTGRES_DB -c "INSERT INTO users (username, login, password) VALUES ('Railgun Admin', 'railgun', '$HASH');"

echo "Initial setup complete"