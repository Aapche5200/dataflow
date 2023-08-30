update t_model set localization='2,1' where name='DAS-NGFW-A800-LU';
update t_model set localization='2,1' where name='DAS-NGFW-A800-FU';
update t_model set localization='2,1' where name='DAS-NGFW-A800S-ZK V3.0';
update t_model set localization='2,1' where name='DAS-NGFW-A800S-HK V3.0';
update t_model set localization='2,1' where name='DAS-NGFW-A1000S-HK V3.0';
update t_model set localization='2,1' where name='DAS-IPS-A800-LU';
update t_model set localization='2,1' where name='DAS-IPS-A800-FU';
update t_model set localization='2,1' where name='WAF-A800-ZN';
update t_model set localization='2,1' where name='WAF-A2000-KU';
update t_model set localization='2,1' where name='WAF-A2180-KU';
update t_model set localization='2,1' where name='WAF-A1200-FU';
update t_model set localization='2,1' where name='WAF-A1500-HU';
update t_model set localization='2,1' where name='WAF-A1680-HU';


    select name,localization,* from t_model where name='DAS-IPS-A800-LU';