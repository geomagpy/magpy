# Utilities
#!/usr/bin/env python

# ---------
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
#import smtplib
from email.mime.text import MIMEText
#from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)
from smtplib import SMTP                  # use this for standard SMTP protocol   (port 25, no encryption)


def send_mail(send_from, send_to, **kwargs):
    """
    Function for sending mails with attachments
    """

    assert type(send_to)==list

    files = kwargs.get('files')
    user = kwargs.get('user')
    pwd = kwargs.get('pwd')
    port = kwargs.get('port')
    smtpserver = kwargs.get('smtpserver')
    subject = kwargs.get('subject')
    text = kwargs.get('text')

    if not smtpserver:
        smtpserver = 'smtp.web.de'
    if not files:
        files = []
    if not text:
        text = 'Cheers, Your Analysis-Robot'
    if not subject:
        subject = 'MagPy - Automatic Analyzer Message'
    if not port:
        port = 587

    assert type(files)==list

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach( MIMEText(text) )

    for f in files:
        part = MIMEBase('application', "octet-stream")
        part.set_payload( open(f,"rb").read() )
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    #smtp = smtplib.SMTP(server)
    smtp = SMTP()
    smtp.set_debuglevel(False)
    smtp.connect(smtpserver, port)
    smtp.ehlo()
    if port == 587:
        smtp.starttls()
    smtp.ehlo()
    if user:
        smtp.login(user, pwd)
    smtp.sendmail(send_from, send_to, msg.as_string())
    smtp.close()

