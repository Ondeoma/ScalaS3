package scalas3.pki

import munit.FunSuite

class PkiSuite extends FunSuite:

  test("CA で署名したノードの証明書は CA で検証でき、名前は要求ではなくノード ID になる") {
    val (caKp, ca) = Pki.createCa("test CA", 30)
    val kp = Pki.newKeyPair()
    val cert = Pki.signCsr(Pki.csr("attacker-chosen", kp), "n1", caKp.getPrivate, ca, 30)
    cert.verify(ca.getPublicKey)
    assertEquals(cert.getSubjectX500Principal.getName, "CN=n1")
    assertEquals(cert.getPublicKey, kp.getPublic)
    assertEquals(cert.getBasicConstraints, -1) // CA ではない
    assert(ca.getBasicConstraints >= 0)
  }

  test("PEM と参加トークンは往復で元に戻る") {
    val (kp, ca) = Pki.createCa("test CA", 30)
    assertEquals(Pki.readCerts(Pki.certPem(ca)), Vector(ca))
    assertEquals(Pki.readKey(Pki.keyPem(kp.getPrivate)), kp.getPrivate)
    val t = JoinToken(Pki.newSecret(), Pki.sha256Hex(ca))
    assertEquals(JoinToken.parse(t.render), Right(t))
    assert(JoinToken.parse("S3J1.x.notahash").isLeft)
  }
