// Updates an existing install of PowerTAC release 0.4 by downloading a 
// set of updated powertac modules from github.
// Usage: groovy updateR0_4.groovy

def originalModules =
  ["powertac-plugins": ["powertac-accounting-service": "release-0.4",
                        "powertac-db-stuff": "release-0.3", 
                        "powertac-distribution-utility": "release-0.4", 
                        "powertac-genco": "release-0.4",
                        "powertac-physical-environment": "release-0.4",
                        "powertac-random": "release-0.3", 
                        "powertac-style": "release-0.3",
                        "powertac-web-app": "release-0.4"]]

def updateModules =
  ["powertac": ["powertac-demo-agent-grails": "release-0.4-UPDATE",
                "powertac-server": "release-0.4-UPDATE"],
   "powertac-plugins": ["powertac-auctioneer-pda": "release-0.4-UPDATE",
                        "powertac-common": "release-0.4-UPDATE", 
                        "powertac-default-broker": "release-0.4-UPDATE",
                        "powertac-household-customer": "release-0.4-UPDATE", 
                        "powertac-server-interface": "release-0.4-UPDATE", 
                        "powertac-visualizer": "release-0.4-UPDATE"]]

// Retrieves a module from github as a tarball, saves to correctly-named file
@Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.5.1')
boolean retrieveModule (String project, String module, String release)
{
  boolean result = false
  println "Retrieve ${module}-${release}"
  def http = new groovyx.net.http.HTTPBuilder("https://github.com/${project}/${module}/tarball/${release}")
  http.request(groovyx.net.http.Method.GET) { req ->

    response.success = { resp, reader ->
      assert resp.status == 200
      println "Response: ${resp.statusLine}, length: ${resp.headers.'Content-Length'}"
      output = new FileOutputStream("${module}.tgz")
      output << reader // print response reader
      output.close()
      result = true
    }
   
    // called only for a 404 (not found) status code:
    response.'404' = { resp -> 
      println 'Not found'
    }
  }

  //def p = "wget https://github.com/${key}/${module}/tarball/${release} -O ${module}.tgz".execute()
  //p.waitFor()
  return result
}

// Extracts the module and renames the directory as expected by grails
def extract (String project, String module)
{
  // check for old dir
  old = new File("${module}")
  if (old.isDirectory()) {
    println "Delete existing dir ${module}"
    old.deleteDir()
  }
  println "Extract ${module}.tgz"
  if (System.properties['os.name'].toLowerCase().contains('windows')) {
    tar = "bsdtar"
  } else {
    tar = "tar"
  }
  cmd = "${tar} -xzf ${module}.tgz"
  p = cmd.execute()
  p.waitFor()
  if (p.exitValue())
    print p.err.text
  new File("${module}.tgz").delete()
  new File('.').listFiles().each { fn ->
    if (fn.isDirectory() && fn.toString() ==~ /\.(\/|\\)${project}-${module}-.*/) {
	  // names start with './'
	  def name = fn.toString()
	  def newName = name[project.size()+3..name.lastIndexOf('-')-1]
	  //println "name=$name, newName=$newName"
	  fn.renameTo(new File(newName))
    }
  }
}

// -- here's the script --

// get branch id from arg
if (args.size() > 0) {
  println "Usage: installR0_4"
  System.exit(1)
}

// Download the original modules only if they do not already exist in the
// local environment
originalModules.each { key, map ->
  map.each { module, release ->
    original = new File("${module}")
    if (original.isDirectory()) {
      println "Dir ${module} exists"
    }
    else {
      if (!retrieveModule(key, module, release)) {
        println "failed to retrieve ${module}"
        new File("${module}.tgz").delete()
      }
      else {
        extract(key, module)
      }
    }
  }
}

// For each updated module, download the tarball, unpack it, and clean up the
// directory name
updateModules.each { key, map ->
  map.each { module, release ->
    if (!retrieveModule(key, module, release)) {
      println "failed to retrieve ${module}"
      new File("${module}.tgz").delete()
    }
    else {
      extract(key, module)
    }
  }
}
