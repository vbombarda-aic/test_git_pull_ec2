sudo crontab -l > mycron
echo "* * * * * cd /home/ec2-user/test_git_pull_ec2/ && sudo git pull" >> mycron
crontab mycron
rm mycron
