<?php

namespace Phinx\Db\Adapter;

use Symfony\Component\Console\Output\OutputInterface;

class MysqlPtOscAdapter extends MysqlAdapter implements AdapterInterface
{
	public function execute($sql)
	{
		// See https://github.com/mbenshoof/liqui-cap-online/ for a similar implementation in Ruby
		$alter_table_regex = '/^\s*ALTER TABLE\s+/i';
		if (preg_match($alter_table_regex, $sql)) {

			$sql = preg_replace($alter_table_regex, '', $sql);

			$options = $this->getOptions();

			$table_identifiers = $this->readTableIdentifier($sql, $alter_statement);
			if (isset($table_identifiers[1])) {
				$database = $table_identifiers[0];
				$table = $table_identifiers[1];
			} else {
				$database = $options['name'];
				$table = $table_identifiers[0];
			}

			$cmd = implode(' ', [
				'pt-online-schema-change',
				'--user ' . $options['user'],
				'--password ' . $options['pass'],
				'--alter "' . $alter_statement . '"',
				'--no-drop-old-table',
				'--execute',
				'D=`' . $database . '`,t=`' . $table . '`',
				'--charset=' . $options['charset'],
			]);

			$this->output->writeln($cmd);
			exec($cmd);
		} else {
			parent::execute($sql);
		}
	}

	private function readTableIdentifier($sql, &$remainder)
	{
		$is_whitespace = function($char) {
			return preg_match('/^\s$/', $char);
		};
		$is_quote = function($char) {
			return $char == '`';
		};
		$is_dot = function ($char) {
			return $char == '.';
		};

		$in_quotes = false;

		$identifiers = [];
		$current_identifier = '';

		$expecting_identifier = true;

		$position = 0;

		for ($i = 0, $j = strlen($sql); $i < $j; $i++) {
			$position++;

			$char = $sql[$i];

			if ($in_quotes) {
				if ($is_quote($char)) {
					$identifiers[] = $current_identifier;
					$current_identifier = '';
					$expecting_identifier = false;
					$in_quotes = false;
				} else {
					$current_identifier .= $char;
				}
			} else {
				if ($is_whitespace($char)) {
					if ($current_identifier != '') {
						$identifiers[] = $current_identifier;
						$current_identifier = '';
						$expecting_identifier = false;
						$in_quotes = false;
					}
					// Skip
				} else if ($expecting_identifier) {
					if ($is_quote($char)) {
						if ($current_identifier == '') {
							$in_quotes = true;
						} else {
							print_r($identifiers);
							throw new \RuntimeException('Could not read schema object identifier from ' . $sql);
						}
					} else if ($is_dot($char)) {
						$identifiers[] = $current_identifier;
						$current_identifier = '';
						$expecting_identifier = true;
					} else {
						$current_identifier .= $char;
					}
				} else {
					// Not expecting identifier
					if ($is_dot($char)) {
						$expecting_identifier = true;
					} else {
						break;
					}
				}
			}
		}

		if ($current_identifier != '') {
			$identifiers[] = $current_identifier;
		}

		if (count($identifiers) > 2) {
			throw new \RuntimeException('Invalid identifier: `' . implode('`.`', $identifiers) . '`');
		}

		$remainder = substr($sql, $position);

		return $identifiers;
	}
}