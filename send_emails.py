import os
import smtplib
from email import encoders
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pprint import pprint


from dotenv import load_dotenv

load_dotenv()


OUTPUT = "./channels/mktg_campaigns"
OUT_SALES = "./channels/sales"
FROM_ADDRESS = "darkfenner69@gmail.com"
TO_ME = "rai_marino@hotmail.com"
TO_JACOPO = "j.a.f.colombo@gmail.com"
TO_ITERA = "e.veneziani@iteradigital.com"
TO_NETING = "m.talentinis.neting.it"
TO_SALES = "leotta@formulacoach.it"
TO_TELEMKTG = "viniero@formulacoach.it"
TO_FORMULA_COACH = "decotiis@formulacoach.it"
SUBJECT = "VirtualVIP RCT sheet update: " + str(datetime.now().date())


def _send_email(
    subject: str,
    from_address: str,
    recipients: list[str],
    content: str,
    filepath: str,
    filename,
) -> None:
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(recipients)
    body = MIMEText(content, "plain")
    msg.attach(body)

    with open(filepath, "rb") as file:
        # Add the attachment to the message
        attachment = MIMEBase("application", "octet-stream")
        attachment.set_payload(file.read())
        encoders.encode_base64(attachment)
        attachment.add_header(
            "Content-Disposition",
            f"attachment; filename= {filename}",
        )

    msg.attach(attachment)

    with smtplib.SMTP_SSL("smtp.gmail.com", port=465) as server:
        server.login(from_address, "secwyxqzjqcqeexb")
        server.sendmail(from_address, recipients, msg.as_string())


def send_email_to_recipients():
    # send email to Business Sales
    sales_path = OUT_SALES + "_" + str(datetime.now().date()) + ".xlsx"
    sales_file = "sales" + "_" + str(datetime.now().date()) + ".xlsx"
    # print(sales_file)
    # print(sales_path)
    recipients = [TO_ME, TO_SALES, TO_TELEMKTG]
    # recipients = [TO_ME]  # for testing

    content = """
        DOMANDE DA COMPLETARE per i clienti con dati parziali:
        1) Campo QUALIFICA
           Risposte possibili:
           HR Manager, Manager (altre funzioni), Imprenditore, Dipendente/Funzionario
        2) Campo SETTORE
           Risposte possibili:
           Servizi-Finanza, Servizi-ICT, Servizi-Consulenza, Servizi-Facilities, Industria, Commercio, P.A., Agricoltura, Altro (specificare il settore specifico)
        3) IMPRESA (dimensione)
           Risposte possibili: 
           1, 2-10, 11-100, 101-250, 251-500, >500
        4) FORMAZIONE
           Risposte possibili:
           Scuola Secondaria, Laurea Breve, Laurea Magistrale, Post-Laurea, Dottorato
        5) Domanda1 (Che tipo di formazione fa la sua azienda?)
           Risposte possibili:
           prevalentemente in aula, prevalentemente online, un mix delle due
        6) Domanda2 (Da quanti anni la sua azienda fa formazione online sulle soft-skill a propri dipendenti?)
           Risposte possibili:
           0, 1-5, >5
        7) Domanda3 (Come vede personalmente l'applicazione di tecnologie emergenti come l'Intelligenza Artificiale alla formazione aziendale?)
           Risposte possibili:
           sono incuriosito, sono sicuramente a favore, mi preoccupa
           
        ISTRUZIONI DI ISCRIZIONE PER OTTENERE LA PROMO:
        Il cliente per usufruire della PROMO dovra' iscriversi cliente BUSINESS utilizzando il COUPON che ha gia' ricevuto via email. 
        In alternativa se non si e' ancora iscritto al Trial Gratuito puo' usare il COUPON Clienti del Business Coach. Il cliente ricevera' comunque la PROMO ma dovra' eseguire la procedura di registrazione iscrivendosi sempre come cliente BUSINESS.
        
        ATTENZIONE:
        In caso il cliente si dovesse iscrivere (oppure si e' gia' iscritto come cliente CONSUMER) non potra' beneficiare della PROMO ma dovra' rieseguire la procedura di registrazione iscrivendosi come cliente BUSINESS per poter beneficiare della PROMO.
        """
    _send_email(SUBJECT, FROM_ADDRESS, recipients, content, sales_path, sales_file)

    # send email to others (myself, Jacopo Colombo, Amalia De Cotiis)
    others_path = OUTPUT + "_" + str(datetime.now().date()) + ".xlsx"
    others_file = "mktg_campaigns" + "_" + str(datetime.now().date()) + ".xlsx"
    recipients = [TO_ME, TO_JACOPO]
    # recipients = [TO_ME] # for testing

    content = """
        LEGENDA:
        applicare la PROMO solo alle emai il cui campo "RCT_group" e' valorizzato come: "Online" oppure "Business_Sales". 
        WARNING: Non applicare la promo al campo RCT_Group il cui valore e' "Control"
        """
    _send_email(SUBJECT, FROM_ADDRESS, recipients, content, others_path, others_file)


if __name__ == "__main__":
    send_email_to_recipients()
