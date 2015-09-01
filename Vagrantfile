# -*- mode: ruby -*-
# vi: set ft=ruby :

VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|

  config.vm.box = "ubuntu/trusty64"
  config.vm.box_url = "http://cloud-images.ubuntu.com/vagrant/precise/current/precise-server-cloudimg-amd64-vagrant-disk1.box"

  config.vm.provision "shell",
  inline: "cd /vagrant/ && ./scripts/init.sh && pip install -e .[test]"

  # mount extra directories
  if ENV['VAGRANT_EXTRA_DIRS']
    extra_dirs = ENV['VAGRANT_EXTRA_DIRS'].split(',')
    for d in extra_dirs
      mount_point = File.basename(d)
      config.vm.synced_folder d, "/opt/" + mount_point
    end
  end

end
