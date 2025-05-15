from PySide6 import QtWidgets, QtGui, QtCore
from utils.mensagem import mensagem_error

class PopupAliquota(QtWidgets.QDialog):
    def __init__(self, codigo, produto, ncm, salvar_callback, progresso, total):
        super().__init__()
        self.setWindowTitle("Itens com Alíquota Nula")
        self.setGeometry(400, 250, 350, 450)
        self.setWindowIcon(QtGui.QIcon(QtGui.QPixmap(1, 1).fill(QtCore.Qt.transparent)))

        self.codigo = codigo
        self.produto = produto
        self.ncm = ncm
        self.salvar_callback = salvar_callback
        self.progresso = progresso
        self.total = total

        self._setup_ui()

    def _setup_ui(self):
        layout = QtWidgets.QVBoxLayout(self)

        layout.addWidget(self._criar_label(f"Código: {self.codigo}"))
        layout.addWidget(self._criar_label(f"Produto: {self.produto}"))
        layout.addWidget(self._criar_label(f"NCM: {self.ncm}"))
        layout.addWidget(self._criar_label("Alíquota:"))

        self.aliquotas = ["1.54%", "4.00%", "8.13%", "ST", "ISENTO"]
        self.button_group = QtWidgets.QButtonGroup()
        checkbox_layout = QtWidgets.QHBoxLayout()

        for aliquota in self.aliquotas:
            checkbox = QtWidgets.QCheckBox(aliquota)
            checkbox.setFont(QtGui.QFont("Arial", 12))
            self.button_group.addButton(checkbox)
            checkbox_layout.addWidget(checkbox)

        self.button_group.setExclusive(True)
        layout.addLayout(checkbox_layout)

        btn_salvar = QtWidgets.QPushButton("Salvar")
        btn_salvar.setStyleSheet("background-color: #28a745; color: white;")
        btn_salvar.clicked.connect(self.salvar_valores)
        layout.addWidget(btn_salvar)

        label_progress = QtWidgets.QLabel(f"Item {self.progresso}/{self.total}")
        label_progress.setFont(QtGui.QFont("Arial", 12))
        layout.addWidget(label_progress)

    def _criar_label(self, texto):
        label = QtWidgets.QLabel(texto)
        label.setFont(QtGui.QFont("Arial", 12))
        return label

    def salvar_valores(self):
        selecionado = self.button_group.checkedButton()
        if selecionado is None:
            mensagem_error("Nenhuma alíquota selecionada.")
            return

        aliquota = selecionado.text()
        self.salvar_callback(self.codigo, aliquota)
        self.accept()