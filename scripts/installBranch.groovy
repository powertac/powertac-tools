def powertacModules =
  ["powertac-demo-agent-grails", "powertac-server"]
def powertacPluginModules =
  ["powertac-accounting-service", "powertac-auctioneer-pda", "powertac-common",
   "powertac-db-stuff", "powertac-default-broker", "powertac-distribution-utility",
   "powertac-genco", "powertac-household-customer", "powertac-physical-environment",
   "powertac-random", "powertac-server-interface", "powertac-style", "powertac-visualizer"]

// get branch id from arg
if (args.size() < 1) {
  println "Usage: installBranch <branch-id>"
  System.exit(1)
}
def release = this.args[0]

// For each module, download the tarball, unpack it, and clean up the directory name
powertacModules.each { module ->
  println "retrieving ${module}-${release}"
  def p = "wget https://github.com/powertac/${module}/tarball/${release} -O ${module}.tgz".execute()
  p.waitFor()
  if (p.exitValue()) {
    println "failed to retrieve ${module}"
  }
  else {
    p = "tar xzf ${module}.tgz".execute()
    p.waitFor()
    new File("${module}.tgz").delete()
    new File('.').listFiles().each { fn ->
      if (fn.isDirectory() && fn.toString() ==~ /.*${module}.*/) {
        def names = fn.toString().split("-")
	def newName = names[1]
	names[2..names.size()-2].each { name ->
          newName += '-'
          newName += name
        }
	fn.renameTo(new File(newName))
      }
    }
  }
}
