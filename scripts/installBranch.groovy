def powertacModules =
  ["powertac": ["powertac-demo-agent-grails", "powertac-server"],
   "powertac-plugins": ["powertac-accounting-service", "powertac-auctioneer-pda",
                        "powertac-common", "powertac-db-stuff", "powertac-default-broker",
                        "powertac-distribution-utility", "powertac-genco",
                        "powertac-household-customer", "powertac-physical-environment",
                        "powertac-random", "powertac-server-interface", "powertac-style",
                        "powertac-visualizer"]]

// get branch id from arg
if (args.size() < 1) {
  println "Usage: installBranch <branch-id>"
  System.exit(1)
}
def release = this.args[0]

// For each module, download the tarball, unpack it, and clean up the directory name
powertacModules.each { key, list ->
  list.each { module ->
    println "retrieving ${module}-${release}"
    def p = "wget https://github.com/${key}/${module}/tarball/${release} -O ${module}.tgz".execute()
    p.waitFor()
    if (p.exitValue()) {
      println "failed to retrieve ${module}"
      new File("${module}.tgz").delete()
    }
    else {
      p = "tar xzf ${module}.tgz".execute()
      p.waitFor()
      new File("${module}.tgz").delete()
      new File('.').listFiles().each { fn ->
        if (fn.isDirectory() && fn.toString() ==~ /\.\/${key}-${module}-.*/) {
	  // names start with './'
	  def name = fn.toString()
	  def newName = name[key.size()+3..name.lastIndexOf('-')-1]
	  println "name=$name, newName=$newName"
	  fn.renameTo(new File(newName))
        }
      }
    }
  }
}
