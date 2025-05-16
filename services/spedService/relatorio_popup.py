from ui.popupAliquota import PopupAliquota

async def tela_popup(nome_banco, progress_bar):
    popup = PopupAliquota(nome_banco)
    popup.exec()
