/*加解密、SHA1数字签名工具*/
package utiltools

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
)

// 先将SOURCE转化为大写，再获取相应的SHA1数字签名
func Sha1Sig(source, token string) string {
	key := []byte(token)
	shamac := hmac.New(sha1.New, key)
	shamac.Write([]byte(strings.ToUpper(source)))
	return fmt.Sprintf("%x", shamac.Sum(nil))
}

// AES 加密字符串
// 秘钥长度128(16bytes)、192、256(32bytes)
// 原文必须填充至blocksize的整数倍，填充方法可以参见https://tools.ietf.org/html/rfc5246#section-6.2.3.2
func AesEncrypter(plaintext, key []byte) string {
	block, err := aes.NewCipher(key) //生成加密用的block
	if err != nil {
		log.Println("AES加密字符串出错：" + err.Error())
		return ""
	}
	plaintext = PKCSPadding(plaintext, block.BlockSize()) //填充数据,不管够不够整除，都添加结束片段
	//采用KEY MD5做初始向量
	md5Val := []byte(fmt.Sprintf("%x", md5.Sum(key)))
	iv := md5Val[0:block.BlockSize()]
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(plaintext))
	mode.CryptBlocks(ciphertext, plaintext)
	return fmt.Sprintf("%x", ciphertext)
}

// AES 解密字符串
func AesDecrypter(encryptedStr string, key []byte) string {
	ciphertext, _ := hex.DecodeString(encryptedStr)
	block, err := aes.NewCipher(key)
	if err != nil {
		log.Println("AES解密字符串出错：" + err.Error())
		return ""
	}
	if len(ciphertext) < aes.BlockSize {
		log.Println("AES解密字符串出错：" + err.Error())
		return ""
	}
	//采用KEY MD5做初始向量
	md5Val := []byte(fmt.Sprintf("%x", md5.Sum(key)))
	iv := md5Val[0:block.BlockSize()]
	if len(ciphertext)%block.BlockSize() != 0 {
		log.Println("AES解密字符串出错：" + err.Error())
		return ""
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	mode.CryptBlocks(ciphertext, ciphertext)
	result := PKCSUnPadding(ciphertext) //删除多余的数据
	return string(result)
}

func PKCSPadding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padText...)
}

func PKCSUnPadding(origData []byte) []byte {
	length := len(origData)
	unpadding := int(origData[length-1])
	return origData[:(length - unpadding)]
}
