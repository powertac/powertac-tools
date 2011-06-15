// Downloads the set of powertac modules from github.
// Usage: groovy installBranch.groovy branch-id

def powertacModules =
  ["powertac": ["powertac-demo-agent-grails", "powertac-server"],
   "powertac-plugins": ["powertac-accounting-service", "powertac-auctioneer-pda",
                        "powertac-common", "powertac-db-stuff", "powertac-default-broker",
                        "powertac-distribution-utility", "powertac-genco",
                        "powertac-household-customer", "powertac-physical-environment",
                        "powertac-random", "powertac-server-interface", "powertac-style",
                        "powertac-visualizer"]]

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
  println "Extract ${module}.tgz"
  p = "tar xzf ${module}.tgz".execute()
  p.waitFor()
  new File("${module}.tgz").delete()
  new File('.').listFiles().each { fn ->
    if (fn.isDirectory() && fn.toString() ==~ /\.\/${project}-${module}-.*/) {
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
if (args.size() < 1) {
  println "Usage: installBranch <branch-id>"
  System.exit(1)
}
def release = this.args[0]

// For each module, download the tarball, unpack it, and clean up the directory name
powertacModules.each { key, list ->
  list.each { module ->
    if (!retrieveModule(key, module, release)) {
      println "failed to retrieve ${module}"
      new File("${module}.tgz").delete()
    }
    else {
      extract(key, module)
    }
  }
}
