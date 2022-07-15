<script setup>
import { ref } from 'vue'
import Sql from '../modelsql.mjs'

defineProps({
  msg: String
})
const modelsqlFallback = {
  statement() {
    return ''
  }
}
const inputValue = ref('')
const jseval = (s) => {
  try {
    return eval(s) || modelsqlFallback
  } catch (error) {
    return modelsqlFallback
  }
}

function onInput(event) {
  inputValue.value = event.target.value
}
</script>

<template>
  <textarea class="form-control" rows="5" cols="100" :value="inputValue" @input="onInput"></textarea>
  <div>{{ jseval(inputValue).statement() }}</div>
</template>

<style scoped>
a {
  color: #42b983;
}
</style>
