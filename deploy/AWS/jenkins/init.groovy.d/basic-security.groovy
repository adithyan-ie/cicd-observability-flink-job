// Creates the admin user on first boot, so the setup wizard (disabled via
// JAVA_OPTS) can be skipped. Change the password from the Jenkins UI after
// first login — editing this file has no effect once the account exists.
import jenkins.model.*
import hudson.security.*

def adminId = 'admin'
def adminPassword = 'admin'

def instance = Jenkins.get()

def hudsonRealm = new HudsonPrivateSecurityRealm(false)
hudsonRealm.createAccount(adminId, adminPassword)
instance.setSecurityRealm(hudsonRealm)

def strategy = new FullControlOnceLoggedInAuthorizationStrategy()
strategy.setAllowAnonymousRead(false)
instance.setAuthorizationStrategy(strategy)

instance.save()
