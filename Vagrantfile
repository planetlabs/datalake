# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "ubuntu/trusty64"

  if ENV['VAGRANT_IP']
    config.vm.network :private_network, ip: ENV['VAGRANT_IP']
  end

  config.vm.provision "shell",
  inline: "cd /vagrant/ && ./init.sh && pip install -e .[test]"

  # mount extra directories
  if ENV['VAGRANT_EXTRA_DIRS']
    extra_dirs = ENV['VAGRANT_EXTRA_DIRS'].split(',')
    for d in extra_dirs
      mount_point = File.basename(d)
      config.vm.synced_folder d, "/opt/" + mount_point
    end
  end

end
